#!/usr/bin/env python3
"""ACLs-as-Code reconciler for Apache Kafka / Confluent Community.

Safety defaults:
- report is the default mode;
- only ALLOW ACLs for principals declared in the YAML are managed;
- DENY ACLs are never deleted;
- cluster ACLs are never deleted unless both --allow-cluster-delete and
  --cluster-delete-approval are supplied;
- ACLs for protected principals are never deleted;
- every real reconcile writes an ACL export and a JSON plan first.

Requires: Python 3.9+ and PyYAML.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from typing import Iterable

try:
    import yaml
except ImportError:
    print("ERROR: falta PyYAML. Instale el paquete corporativo python3-pyyaml.", file=sys.stderr)
    raise SystemExit(2)

VALID_PATTERN_TYPES = {"literal", "prefixed"}
VALID_PERMISSION_TYPES = {"ALLOW", "DENY"}
VALID_OPERATIONS = {
    "All", "Alter", "AlterConfigs", "ClusterAction", "Create", "Delete",
    "Describe", "DescribeConfigs", "IdempotentWrite", "Read", "Write"
}
OPS_BY_RESOURCE = {
    "TOPIC": {"All", "Alter", "AlterConfigs", "Create", "Delete", "Describe", "DescribeConfigs", "Read", "Write"},
    "GROUP": {"All", "Delete", "Describe", "Read"},
    "CLUSTER": {"All", "Alter", "AlterConfigs", "ClusterAction", "Create", "Describe", "DescribeConfigs", "IdempotentWrite"},
    "TRANSACTIONAL_ID": {"All", "Describe", "Write"},
}
YAML_RESOURCES = {
    "topics": ("TOPIC", "--topic", True),
    "consumerGroups": ("GROUP", "--group", True),
    "clusters": ("CLUSTER", "--cluster", False),
    "transactionalIds": ("TRANSACTIONAL_ID", "--transactional-id", True),
}
RESOURCE_FLAGS = {
    "TOPIC": "--topic",
    "GROUP": "--group",
    "CLUSTER": "--cluster",
    "TRANSACTIONAL_ID": "--transactional-id",
}
# Never remove ACLs for these unless the source code is deliberately changed.
BUILTIN_PROTECTED_PRINCIPALS = frozenset({"User:kafka"})
CLUSTER_DELETE_APPROVAL = "I_UNDERSTAND_CLUSTER_ACL_DELETION"


class ConfigError(Exception):
    pass


@dataclasses.dataclass(frozen=True, order=True)
class Acl:
    principal: str
    resource_type: str
    resource_name: str
    pattern_type: str
    operation: str
    permission_type: str = "ALLOW"
    host: str = "*"

    def label(self) -> str:
        return (f"{self.permission_type} {self.principal} {self.operation} "
                f"{self.resource_type}:{self.resource_name} "
                f"pattern={self.pattern_type} host={self.host}")


def die(message: str, code: int = 2) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(code)


def run(cmd: list[str], *, capture: bool = False, input_text: str | None = None) -> str:
    try:
        cp = subprocess.run(cmd, check=True, text=True, input=input_text,
                            capture_output=capture)
    except FileNotFoundError:
        die(f"No se encuentra el ejecutable: {cmd[0]}")
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            print(exc.stdout, file=sys.stderr)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        die(f"Fallo del comando ({exc.returncode}): {shlex.join(cmd)}", exc.returncode)
    return cp.stdout if capture else ""


def require_string(obj: dict, key: str, where: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}.{key} debe ser texto no vacio")
    return value.strip()


def load_yaml(path: Path) -> dict:
    try:
        with path.open(encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"No se puede leer {path}: {exc}") from exc
    if not isinstance(doc, dict):
        raise ConfigError("La raiz YAML debe ser un mapa")
    return doc


def validate_and_build_desired(doc: dict) -> tuple[dict, set[Acl], set[str], set[str]]:
    cluster = doc.get("cluster")
    if not isinstance(cluster, dict):
        raise ConfigError("Falta el mapa raiz cluster")
    name = require_string(cluster, "name", "cluster")
    bootstrap = require_string(cluster, "bootstrap", "cluster")
    command_config = require_string(cluster, "command_config", "cluster")
    if not Path(command_config).is_file():
        raise ConfigError(f"command_config no existe o no es fichero: {command_config}")
    if "dry_run" in cluster and not isinstance(cluster["dry_run"], bool):
        raise ConfigError("cluster.dry_run debe ser booleano")

    principals = doc.get("principals")
    if not isinstance(principals, list) or not principals:
        raise ConfigError("principals debe ser una lista no vacia")

    desired: set[Acl] = set()
    managed_principals: set[str] = set()
    duplicates: set[str] = set()
    for p_idx, principal_obj in enumerate(principals):
        where = f"principals[{p_idx}]"
        if not isinstance(principal_obj, dict):
            raise ConfigError(f"{where} debe ser mapa")
        principal = require_string(principal_obj, "name", where)
        if not principal.startswith("User:"):
            raise ConfigError(f"{where}.name debe comenzar por User: ({principal})")
        if principal in managed_principals:
            raise ConfigError(f"Principal duplicado: {principal}")
        managed_principals.add(principal)
        permissions = principal_obj.get("permissions")
        if not isinstance(permissions, list) or not permissions:
            raise ConfigError(f"{where}.permissions debe ser lista no vacia")

        for b_idx, block in enumerate(permissions):
            bwhere = f"{where}.permissions[{b_idx}]"
            if not isinstance(block, dict) or not block:
                raise ConfigError(f"{bwhere} debe ser mapa no vacio")
            unknown = set(block) - set(YAML_RESOURCES)
            if unknown:
                raise ConfigError(f"{bwhere}: recursos no soportados: {sorted(unknown)}")
            for yaml_key, resources in block.items():
                rtype, _, needs_pattern = YAML_RESOURCES[yaml_key]
                if not isinstance(resources, list) or not resources:
                    raise ConfigError(f"{bwhere}.{yaml_key} debe ser lista no vacia")
                for r_idx, resource in enumerate(resources):
                    rwhere = f"{bwhere}.{yaml_key}[{r_idx}]"
                    if not isinstance(resource, dict):
                        raise ConfigError(f"{rwhere} debe ser mapa")
                    rname = require_string(resource, "name", rwhere)
                    if rtype == "CLUSTER" and rname != "kafka-cluster":
                        raise ConfigError(f"{rwhere}.name debe ser kafka-cluster")
                    if needs_pattern:
                        pattern = require_string(resource, "patternType", rwhere).lower()
                        if pattern not in VALID_PATTERN_TYPES:
                            raise ConfigError(f"{rwhere}.patternType invalido: {pattern}")
                        if "*" in rname and rname != "*":
                            raise ConfigError(f"{rwhere}.name no puede contener '*'; use prefixed sin asterisco")
                        if rname == "*" and pattern != "literal":
                            raise ConfigError(f"{rwhere}: '*' solo se admite con patternType literal")
                    else:
                        if "patternType" in resource:
                            raise ConfigError(f"{rwhere}: cluster no debe declarar patternType")
                        pattern = "literal"
                    operations = resource.get("operations")
                    if not isinstance(operations, list) or not operations:
                        raise ConfigError(f"{rwhere}.operations debe ser lista no vacia")
                    if len(operations) != len(set(operations)):
                        raise ConfigError(f"{rwhere}: operaciones duplicadas")
                    for op in operations:
                        if op not in VALID_OPERATIONS or op not in OPS_BY_RESOURCE[rtype]:
                            raise ConfigError(f"{rwhere}: operacion {op!r} no valida para {rtype}")
                        acl = Acl(principal, rtype, rname, pattern, op)
                        if acl in desired:
                            duplicates.add(acl.label())
                        desired.add(acl)
    if duplicates:
        raise ConfigError("ACLs duplicadas:\n  " + "\n  ".join(sorted(duplicates)))

    protected = set(BUILTIN_PROTECTED_PRINCIPALS)
    policy = doc.get("reconcile", {})
    if policy is not None and not isinstance(policy, dict):
        raise ConfigError("reconcile debe ser mapa")
    if isinstance(policy, dict):
        configured = policy.get("protectedPrincipals", [])
        if not isinstance(configured, list) or not all(isinstance(x, str) and x for x in configured):
            raise ConfigError("reconcile.protectedPrincipals debe ser lista de textos")
        protected.update(configured)
    return {"name": name, "bootstrap": bootstrap, "command_config": command_config}, desired, managed_principals, protected


# kafka-acls --list output parser for the standard Kafka CLI human-readable format.
RESOURCE_RE = re.compile(
    r"Current ACLs for resource `ResourcePattern\(resourceType=(?P<type>[A-Z_]+), "
    r"name=(?P<name>.*?), patternType=(?P<pattern>[A-Z_]+)\)`"
)
ENTRY_RE = re.compile(
    r"\(principal=(?P<principal>.*?), host=(?P<host>.*?), "
    r"operation=(?P<operation>[A-Z_]+), permissionType=(?P<permission>[A-Z_]+)\)"
)
CLI_TO_YAML_OP = {
    "ALL": "All", "ALTER": "Alter", "ALTER_CONFIGS": "AlterConfigs",
    "CLUSTER_ACTION": "ClusterAction", "CREATE": "Create", "DELETE": "Delete",
    "DESCRIBE": "Describe", "DESCRIBE_CONFIGS": "DescribeConfigs",
    "IDEMPOTENT_WRITE": "IdempotentWrite", "READ": "Read", "WRITE": "Write",
}


def parse_current_acls(text: str) -> set[Acl]:
    current: set[Acl] = set()
    active: tuple[str, str, str] | None = None
    saw_resource = False
    for raw in text.splitlines():
        line = raw.strip()
        rm = RESOURCE_RE.search(line)
        if rm:
            saw_resource = True
            rtype = rm.group("type")
            rname = rm.group("name")
            pattern = rm.group("pattern").lower()
            if rtype not in RESOURCE_FLAGS:
                active = None
            else:
                active = (rtype, rname, pattern)
            continue
        em = ENTRY_RE.search(line)
        if em and active:
            operation = CLI_TO_YAML_OP.get(em.group("operation"))
            permission = em.group("permission")
            if operation is None or permission not in VALID_PERMISSION_TYPES:
                raise ConfigError(f"ACL actual no reconocida: {line}")
            current.add(Acl(
                principal=em.group("principal"), resource_type=active[0],
                resource_name=active[1], pattern_type=active[2], operation=operation,
                permission_type=permission, host=em.group("host")))
    # Empty text is not a valid successful export. Text with no resources may be a truly empty cluster.
    if not text.strip():
        raise ConfigError("La salida de kafka-acls --list esta vacia")
    if "Current ACLs for resource" in text and not saw_resource:
        raise ConfigError("No se pudo interpretar la salida de kafka-acls --list")
    return current


def base_cli(cluster: dict, executable: str) -> list[str]:
    return [executable, "--bootstrap-server", cluster["bootstrap"],
            "--command-config", cluster["command_config"]]


def export_current(cluster: dict, executable: str) -> str:
    return run(base_cli(cluster, executable) + ["--list"], capture=True)


def acl_command(cluster: dict, executable: str, action: str, acl: Acl) -> list[str]:
    if acl.permission_type != "ALLOW" or acl.host != "*":
        raise ConfigError("Este reconciliador solo crea/elimina ACL ALLOW con host '*'")
    cmd = base_cli(cluster, executable) + [action, "--allow-principal", acl.principal,
          "--operation", acl.operation]
    if acl.resource_type == "CLUSTER":
        cmd.append("--cluster")
    else:
        cmd += [RESOURCE_FLAGS[acl.resource_type], acl.resource_name,
                "--resource-pattern-type", acl.pattern_type]
    if action == "--remove":
        cmd += ["--force"]
    return cmd


def make_plan(desired: set[Acl], current: set[Acl], managed: set[str], protected: set[str],
              allow_cluster_delete: bool) -> tuple[list[Acl], list[Acl], list[tuple[Acl, str]]]:
    comparable_current = {a for a in current if a.permission_type == "ALLOW" and a.host == "*"}
    to_add = sorted(desired - comparable_current)
    candidates = sorted(a for a in comparable_current - desired if a.principal in managed)
    to_remove: list[Acl] = []
    blocked: list[tuple[Acl, str]] = []
    for acl in candidates:
        if acl.principal in protected:
            blocked.append((acl, "principal protegido"))
        elif acl.resource_type == "CLUSTER" and not allow_cluster_delete:
            blocked.append((acl, "el borrado de ACL de cluster esta bloqueado"))
        else:
            to_remove.append(acl)
    return to_add, to_remove, blocked


def print_plan(to_add: list[Acl], to_remove: list[Acl], blocked: list[tuple[Acl, str]]) -> None:
    print(f"ACL faltantes: {len(to_add)}")
    for acl in to_add:
        print(f"  + {acl.label()}")
    print(f"ACL sobrantes eliminables: {len(to_remove)}")
    for acl in to_remove:
        print(f"  - {acl.label()}")
    print(f"ACL sobrantes protegidas/bloqueadas: {len(blocked)}")
    for acl, reason in blocked:
        print(f"  ! {acl.label()} [{reason}]")


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent), text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def main() -> int:
    ap = argparse.ArgumentParser(description="Reporta, aplica o reconcilia ACLs Kafka desde YAML")
    ap.add_argument("yaml_file", type=Path)
    ap.add_argument("--mode", choices=("report", "apply", "reconcile"), default="report")
    ap.add_argument("--execute", action="store_true", help="Obligatorio para efectuar cambios reales")
    ap.add_argument("--kafka-acls-bin", default="kafka-acls")
    ap.add_argument("--backup-dir", type=Path, default=Path("backup"))
    ap.add_argument("--current-acls-file", type=Path, help="Lee una exportacion para pruebas; incompatible con --execute")
    ap.add_argument("--protected-principal", action="append", default=[])
    ap.add_argument("--allow-cluster-delete", action="store_true")
    ap.add_argument("--cluster-delete-approval", default="")
    ap.add_argument("--max-deletes", type=int, default=20)
    args = ap.parse_args()

    if args.max_deletes < 0:
        die("--max-deletes no puede ser negativo")
    if args.execute and args.mode == "report":
        die("--execute no tiene sentido con --mode report")
    if args.execute and args.current_acls_file:
        die("--current-acls-file no puede usarse con --execute")
    if args.allow_cluster_delete and args.cluster_delete_approval != CLUSTER_DELETE_APPROVAL:
        die(f"Para borrar ACL de cluster debe incluir --cluster-delete-approval {CLUSTER_DELETE_APPROVAL}")
    #if args.mode == "reconcile" and args.execute and os.geteuid() == 0:
    #    die("Por seguridad, reconcile no debe ejecutarse como root")
    if args.mode == "reconcile" and args.execute and os.geteuid() == 0:
        print("WARNING: Ejecutando reconcile como root",file=sys.stderr)
    
    try:
        doc = load_yaml(args.yaml_file)
        cluster, desired, managed, protected = validate_and_build_desired(doc)
        protected.update(args.protected_principal)
        if shutil.which(args.kafka_acls_bin) is None and not args.current_acls_file:
            raise ConfigError(f"No se encuentra {args.kafka_acls_bin} en PATH")
        if args.current_acls_file:
            current_text = args.current_acls_file.read_text(encoding="utf-8")
        else:
            current_text = export_current(cluster, args.kafka_acls_bin)
        current = parse_current_acls(current_text)
        to_add, to_remove, blocked = make_plan(
            desired, current, managed, protected, args.allow_cluster_delete)
    except (ConfigError, OSError) as exc:
        die(str(exc))

    print(f"Cluster YAML: {cluster['name']} ({cluster['bootstrap']})")
    print(f"Principals gestionados: {len(managed)}")
    print(f"Principals protegidos: {', '.join(sorted(protected))}")
    print_plan(to_add, to_remove, blocked)

    if args.mode == "report" or not args.execute:
        print("DRY-RUN: no se ha realizado ningun cambio. Use --execute para aplicar.")
        return 1 if (to_add or (args.mode == "reconcile" and to_remove)) else 0

    if args.mode == "apply":
        to_remove = []

    if len(to_remove) > args.max_deletes:
        die(f"El plan intenta borrar {len(to_remove)} ACLs y supera --max-deletes={args.max_deletes}")

    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    args.backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = args.backup_dir / f"acls_before_{cluster['name']}_{timestamp}.txt"
    plan_path = args.backup_dir / f"reconcile_plan_{cluster['name']}_{timestamp}.json"
    atomic_write(backup_path, current_text)
    plan = {
        "timestamp_utc": timestamp,
        "cluster": cluster["name"], "bootstrap": cluster["bootstrap"],
        "mode": args.mode,
        "add": [dataclasses.asdict(a) for a in to_add],
        "remove": [dataclasses.asdict(a) for a in to_remove],
        "blocked": [{"acl": dataclasses.asdict(a), "reason": reason} for a, reason in blocked],
    }
    atomic_write(plan_path, json.dumps(plan, indent=2, ensure_ascii=False) + "\n")
    print(f"Backup: {backup_path}")
    print(f"Plan: {plan_path}")

    # Add first to minimize accidental loss of access; remove only after additions succeed.
    for acl in to_add:
        cmd = acl_command(cluster, args.kafka_acls_bin, "--add", acl)
        print(f"EJECUTANDO: {shlex.join(cmd)}")
        run(cmd)
    for acl in to_remove:
        cmd = acl_command(cluster, args.kafka_acls_bin, "--remove", acl)
        print(f"EJECUTANDO: {shlex.join(cmd)}")
        run(cmd)

    # Mandatory post-check.
    after_text = export_current(cluster, args.kafka_acls_bin)
    after = parse_current_acls(after_text)
    remaining_add, remaining_remove, remaining_blocked = make_plan(
        desired, after, managed, protected, args.allow_cluster_delete)
    after_path = args.backup_dir / f"acls_after_{cluster['name']}_{timestamp}.txt"
    atomic_write(after_path, after_text)
    if remaining_add or (args.mode == "reconcile" and remaining_remove):
        print_plan(remaining_add, remaining_remove, remaining_blocked)
        die("La verificacion posterior detecta deriva restante", 4)
    print(f"OK: verificacion posterior correcta. Export final: {after_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        die("Ejecucion interrumpida", 130)
