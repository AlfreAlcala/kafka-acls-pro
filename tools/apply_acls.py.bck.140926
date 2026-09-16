#!/usr/bin/env python3
import argparse
import subprocess
import shlex
from pathlib import Path
import re
import yaml

OP_ALLOWED = {
    "Read","Write","Describe","Create","Delete","Alter",
    "DescribeConfigs","AlterConfigs","ClusterAction","IdempotentWrite","All",
    # opcionales según versión, si los usas puedes añadirlos:
    # "DescribeTokens","CreateTokens","TwoPhaseCommit"
}

RESOURCE_FLAG = {
    "topics": "--topic",
    "consumerGroups": "--group",
    "clusters": "--cluster",
    "transactionalIds": "--transactional-id",
    "delegationTokens": "--delegation-token",
}

# Kafka-acls --add solo acepta patternType específico (LITERAL/PREFIXED).
PATTERN_FLAG = {
    "literal": "literal",
    "prefixed": "prefixed",
}

# Para comparar contra la salida de kafka-acls --list (resourceType=...)
SECTION_TO_RTYPE = {
    "topics": "TOPIC",
    "consumerGroups": "GROUP",
    "clusters": "CLUSTER",
    "transactionalIds": "TRANSACTIONAL_ID",
    "delegationTokens": "DELEGATION_TOKEN",
}

# Parse de salida típica de `kafka-acls --list`
RE_RESOURCE = re.compile(
    r"ResourcePattern\(resourceType=(?P<rtype>[^,]+),\s*name=(?P<rname>[^,]+),\s*patternType=(?P<ptype>[^)]+)\)",
    re.IGNORECASE
)

RE_ENTRY = re.compile(
    r"\(principal=(?P<principal>[^,]+),\s*host=(?P<host>[^,]+),\s*operation=(?P<op>[^,]+),\s*permissionType=(?P<perm>[^)]+)\)",
    re.IGNORECASE
)


def run(cmd, dry):
    # Print a shell-safe representation so wildcards like '*' are not expanded when copy/pasted.
    try:
        printable = shlex.join(cmd)  # Python 3.8+
    except AttributeError:
        printable = ' '.join(shlex.quote(c) for c in cmd)
    print(printable)
    if dry:
        return 0
    return subprocess.run(cmd, check=False).returncode
def run_capture(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise SystemExit(f"ERROR ejecutando: {' '.join(cmd)}\n{p.stdout}")
    return p.stdout


def acl_key(rtype, rname, ptype, principal, host, op, perm):
    return (
        rtype.strip().upper(),
        rname.strip(),
        ptype.strip().upper(),
        principal.strip(),
        host.strip(),
        op.strip().upper(),
        perm.strip().upper(),
    )


def load_existing_acls(bootstrap, command_cfg):
    out = run_capture([
        "kafka-acls",
        "--bootstrap-server", bootstrap,
        "--command-config", command_cfg,
        "--list"
    ])

    existing = set()
    current_resource = None  # (rtype, rname, ptype)

    for line in out.splitlines():
        mres = RE_RESOURCE.search(line)
        if mres:
            current_resource = (
                mres.group("rtype"),
                mres.group("rname"),
                mres.group("ptype"),
            )
            continue

        ment = RE_ENTRY.search(line)
        if ment and current_resource:
            rtype, rname, ptype = current_resource
            existing.add(
                acl_key(
                    rtype, rname, ptype,
                    ment.group("principal"),
                    ment.group("host"),
                    ment.group("op"),
                    ment.group("perm"),
                )
            )
    return existing


def normalize_pattern_type(ptype, name):
    """
    Solo literal/prefixed son válidos en --add.
    Wildcard real: name='*' con patternType literal.
    """
    p = (ptype or "literal").strip().lower()
    if p == "wildcard":
        raise SystemExit("patternType 'wildcard' no se soporta: usa name='*' con patternType 'literal'.")
    if p not in ("literal", "prefixed"):
        raise SystemExit(f"patternType no soportado: {ptype} (solo literal/prefixed)")
    # si name='*', recomendamos literal (y si viene prefixed lo dejamos, pero avisarías en lint)
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("env_yaml", help="env/dev.yaml")
    ap.add_argument("--mode", choices=["apply","dry-run"], default="dry-run")
    ap.add_argument("--skip-existing", action="store_true",
                    help="Salta ejecutar kafka-acls --add si la ACL ya existe (cachea kafka-acls --list).")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.env_yaml).read_text())
    cluster = cfg["cluster"]
    bootstrap = cluster["bootstrap"]
    command_cfg = cluster["command_config"]
    dry = (args.mode != "apply") or bool(cluster.get("dry_run", False))

    # Cache de ACLs existentes (solo tiene sentido en apply real)
    existing = set()
    if (not dry) and args.skip_existing:
        existing = load_existing_acls(bootstrap, command_cfg)

    for p in cfg.get("principals", []):
        principal = p["name"]

        for perm in p.get("permissions", []):
            for section, flag in RESOURCE_FLAG.items():
                for entry in (perm.get(section, []) or []):
                    name = entry.get("name")

                    if not name:
                        raise SystemExit(f"Entrada sin name en section={section} principal={principal}")

                    # clusters a veces no trae patternType
                    ptype = normalize_pattern_type(entry.get("patternType", "literal"), name)

                    ops = entry.get("operations", [])
                    for op in ops:
                        if op not in OP_ALLOWED:
                            raise SystemExit(f"Operacion no soportada: {op}")

                        # Construir comando
                        cmd = [
                            "kafka-acls",
                            "--bootstrap-server", bootstrap,
                            "--command-config", command_cfg,
                            "--add",
                            "--allow-principal", principal,
                            "--operation", op,
                            flag, name,
                        ]

                        # Solo pasar resource-pattern-type si es prefixed (literal es default)
                        if ptype == "prefixed":
                            cmd += ["--resource-pattern-type", PATTERN_FLAG["prefixed"]]

                        # SKIP si ya existe (solo en apply real)
                        if (not dry) and args.skip_existing:
                            rtype = SECTION_TO_RTYPE.get(section, section.upper())
                            host = "*"          # default allow-host cuando no se pasa --allow-host
                            perm_type = "ALLOW" # porque usamos --allow-principal
                            ptype_norm = ptype.upper()

                            candidate = acl_key(rtype, name, ptype_norm, principal, host, op, perm_type)

                            if candidate in existing:
                                print(f"SKIP (ya existe): {principal} {op} {rtype}:{ptype_norm}:{name}")
                                continue

                        rc = run(cmd, dry)
                        if rc != 0:
                            raise SystemExit(rc)

                        # Añade al cache para evitar duplicados dentro de la misma ejecución
                        if (not dry) and args.skip_existing:
                            existing.add(candidate)


if __name__ == "__main__":
    main()
