#!/usr/bin/env python3

import argparse
import csv
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq


RESOURCE_CONFIG = {
    "topic": {
        "yaml_key": "topics",
        "requires_pattern": True,
    },
    "consumerGroup": {
        "yaml_key": "consumerGroups",
        "requires_pattern": True,
    },
    "cluster": {
        "yaml_key": "clusters",
        "requires_pattern": False,
    },
    "transactionalId": {
        "yaml_key": "transactionalIds",
        "requires_pattern": True,
    },
}

VALID_PATTERNS = {
    "literal",
    "prefixed",
}

VALID_OPERATIONS = {
    "All",
    "Alter",
    "AlterConfigs",
    "ClusterAction",
    "Create",
    "Delete",
    "Describe",
    "DescribeConfigs",
    "IdempotentWrite",
    "Read",
    "Write",
}

# Validación conservadora adaptada a los recursos presentes
# en vuestro YAML actual.
VALID_OPERATIONS_BY_RESOURCE = {
    "topic": {
        "All",
        "Alter",
        "AlterConfigs",
        "Create",
        "Delete",
        "Describe",
        "DescribeConfigs",
        "Read",
        "Write",
    },
    "consumerGroup": {
        "All",
        "Delete",
        "Describe",
        "Read",
    },
    "cluster": {
        "All",
        "Alter",
        "AlterConfigs",
        "ClusterAction",
        "Create",
        "Describe",
        "DescribeConfigs",
        "IdempotentWrite",
    },
    "transactionalId": {
        "All",
        "Describe",
        "Write",
    },
}

REQUIRED_COLUMNS = {
    "ticket",
    "principal",
    "resourceType",
    "name",
    "patternType",
    "operations",
}


class ValidationError(Exception):
    pass


def build_yaml():
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.default_flow_style = None
    yaml.width = 4096
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Importa peticiones de ACL desde CSV al YAML "
            "ACLs-as-Code y opcionalmente crea un commit Git."
        )
    )

    parser.add_argument(
        "--csv",
        required=True,
        help="CSV que contiene las peticiones.",
    )

    parser.add_argument(
        "--yaml",
        required=True,
        help="YAML que se va a modificar, por ejemplo env/pro.yaml.",
    )

    parser.add_argument(
        "--ticket",
        help=(
            "Ticket esperado. Si se indica, todas las filas del CSV "
            "deben corresponder a este ticket."
        ),
    )

    parser.add_argument(
        "--backup-dir",
        default="/opt/kafka/backup/",
        help="Directorio para backups. Por defecto: /opt/kafka/backup/.",
    )

    parser.add_argument(
        "--git-commit",
        action="store_true",
        help="Crea rama, git add y git commit después de validar.",
    )

    parser.add_argument(
        "--branch",
        help=(
            "Nombre de la rama Git. Si no se indica, se genera "
            "acl/<ticket-normalizado>."
        ),
    )

    parser.add_argument(
        "--commit-message",
        help=(
            "Mensaje de commit. Si no se indica, se genera a partir "
            "del ticket."
        ),
    )

    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Valida CSV y YAML, pero no modifica el fichero.",
    )

    return parser.parse_args()


def fail(message):
    raise ValidationError(message)


def normalize_ticket_for_branch(ticket):
    value = ticket.strip().lower()
    value = re.sub(r"[^a-z0-9._-]+", "-", value)
    value = value.strip("-")
    return value or "sin-ticket"


def run_git(args, capture_output=False):
    result = subprocess.run(
        ["git", *args],
        check=True,
        text=True,
        capture_output=capture_output,
    )

    if capture_output:
        return result.stdout.strip()

    return ""


def ensure_git_repository():
    try:
        inside = run_git(
            ["rev-parse", "--is-inside-work-tree"],
            capture_output=True,
        )
    except subprocess.CalledProcessError as error:
        fail(
            "No se ha encontrado un repositorio Git. "
            "Ejecuta el script desde el repositorio kafka-acls."
        )

    if inside != "true":
        fail("El directorio actual no es un repositorio Git.")


def ensure_clean_target_file(yaml_path):
    relative_path = os.path.relpath(yaml_path, Path.cwd())

    status = run_git(
        ["status", "--porcelain", "--", relative_path],
        capture_output=True,
    )

    if status:
        fail(
            f"El fichero {relative_path} ya tiene cambios sin commit. "
            "Guárdalos o revísalos antes de importar el CSV."
        )


def load_yaml(yaml_engine, yaml_path):
    try:
        with yaml_path.open("r", encoding="utf-8") as stream:
            data = yaml_engine.load(stream)
    except Exception as error:
        fail(f"No se puede leer el YAML {yaml_path}: {error}")

    if data is None:
        fail(f"El YAML {yaml_path} está vacío.")

    if not isinstance(data, dict):
        fail("La raíz del YAML debe ser un mapa.")

    return data


def validate_cluster_header(data):
    cluster = data.get("cluster")

    if not isinstance(cluster, dict):
        fail("Falta el bloque raíz 'cluster' o no es un mapa.")

    required = {
        "name",
        "bootstrap",
        "command_config",
        "dry_run",
        "source",
    }

    missing = sorted(required - set(cluster.keys()))

    if missing:
        fail(
            "Faltan campos en el bloque cluster: "
            + ", ".join(missing)
        )

    if not isinstance(cluster.get("dry_run"), bool):
        fail("cluster.dry_run debe ser true o false, sin comillas.")


def validate_resource(resource_type, resource, principal_name):
    if not isinstance(resource, dict):
        fail(
            f"Recurso inválido para {principal_name}: "
            "cada recurso debe ser un mapa."
        )

    name = resource.get("name")

    if not isinstance(name, str) or not name.strip():
        fail(
            f"Recurso sin nombre válido para {principal_name}."
        )

    config = RESOURCE_CONFIG[resource_type]

    if config["requires_pattern"]:
        pattern = resource.get("patternType")

        if pattern not in VALID_PATTERNS:
            fail(
                f"patternType inválido en {principal_name}, "
                f"{resource_type} {name}: {pattern!r}."
            )
    elif "patternType" in resource:
        fail(
            f"El recurso cluster {name} de {principal_name} "
            "no debe contener patternType con el modelo actual."
        )

    operations = resource.get("operations")

    if not isinstance(operations, list) or not operations:
        fail(
            f"El recurso {resource_type} {name} de "
            f"{principal_name} no tiene operaciones."
        )

    duplicated_operations = [
        operation
        for operation in set(operations)
        if operations.count(operation) > 1
    ]

    if duplicated_operations:
        fail(
            f"Operaciones duplicadas en {principal_name}, "
            f"{resource_type} {name}: "
            + ", ".join(sorted(duplicated_operations))
        )

    allowed = VALID_OPERATIONS_BY_RESOURCE[resource_type]

    invalid = [
        operation
        for operation in operations
        if operation not in VALID_OPERATIONS
        or operation not in allowed
    ]

    if invalid:
        fail(
            f"Operaciones inválidas para {resource_type} "
            f"{name} de {principal_name}: "
            + ", ".join(invalid)
        )


def validate_yaml_model(data):
    validate_cluster_header(data)

    principals = data.get("principals")

    if not isinstance(principals, list):
        fail("El bloque 'principals' debe ser una lista.")

    seen_principals = set()
    seen_resources = set()

    yaml_key_to_resource = {
        config["yaml_key"]: resource_type
        for resource_type, config in RESOURCE_CONFIG.items()
    }

    for principal in principals:
        if not isinstance(principal, dict):
            fail("Cada principal debe ser un mapa.")

        principal_name = principal.get("name")

        if not isinstance(principal_name, str):
            fail("Se ha encontrado un principal sin nombre válido.")

        if not principal_name.startswith("User:"):
            fail(
                f"El principal {principal_name!r} debe comenzar por 'User:'."
            )

        if principal_name in seen_principals:
            fail(f"Principal duplicado: {principal_name}")

        seen_principals.add(principal_name)

        permissions = principal.get("permissions")

        if not isinstance(permissions, list):
            fail(
                f"permissions de {principal_name} debe ser una lista."
            )

        for block in permissions:
            if not isinstance(block, dict):
                fail(
                    f"Bloque permissions inválido en {principal_name}."
                )

            unknown_keys = (
                set(block.keys()) - set(yaml_key_to_resource.keys())
            )

            if unknown_keys:
                fail(
                    f"Claves de permisos desconocidas en "
                    f"{principal_name}: "
                    + ", ".join(sorted(unknown_keys))
                )

            for yaml_key, resources in block.items():
                resource_type = yaml_key_to_resource[yaml_key]

                if not isinstance(resources, list):
                    fail(
                        f"{yaml_key} de {principal_name} "
                        "debe ser una lista."
                    )

                for resource in resources:
                    validate_resource(
                        resource_type,
                        resource,
                        principal_name,
                    )

                    signature = (
                        principal_name,
                        resource_type,
                        resource["name"],
                        resource.get("patternType"),
                    )

                    if signature in seen_resources:
                        fail(
                            "Recurso duplicado en YAML: "
                            f"{principal_name}, {resource_type}, "
                            f"{resource['name']}, "
                            f"{resource.get('patternType')}"
                        )

                    seen_resources.add(signature)


def read_csv_rows(csv_path, expected_ticket=None):
    try:
        stream = csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        )
    except OSError as error:
        fail(f"No se puede abrir el CSV {csv_path}: {error}")

    rows = []

    with stream:
        reader = csv.DictReader(stream)

        if reader.fieldnames is None:
            fail("El CSV no contiene cabecera.")

        missing_columns = (
            REQUIRED_COLUMNS - set(reader.fieldnames)
        )

        if missing_columns:
            fail(
                "Faltan columnas en el CSV: "
                + ", ".join(sorted(missing_columns))
            )

        for line_number, raw_row in enumerate(reader, start=2):
            row = {
                key: (value or "").strip()
                for key, value in raw_row.items()
            }

            if not any(row.values()):
                continue

            ticket = row["ticket"]
            principal = row["principal"]
            resource_type = row["resourceType"]
            name = row["name"]
            pattern = row["patternType"]

            if not ticket:
                fail(f"Línea {line_number}: ticket vacío.")

            if expected_ticket and ticket != expected_ticket:
                fail(
                    f"Línea {line_number}: ticket {ticket!r} "
                    f"distinto de {expected_ticket!r}."
                )

            if not principal.startswith("User:"):
                fail(
                    f"Línea {line_number}: el principal debe "
                    "comenzar por 'User:'."
                )

            if resource_type not in RESOURCE_CONFIG:
                fail(
                    f"Línea {line_number}: resourceType inválido "
                    f"{resource_type!r}."
                )

            if not name:
                fail(
                    f"Línea {line_number}: nombre de recurso vacío."
                )

            requires_pattern = (
                RESOURCE_CONFIG[resource_type]["requires_pattern"]
            )

            if requires_pattern and pattern not in VALID_PATTERNS:
                fail(
                    f"Línea {line_number}: patternType debe ser "
                    "'literal' o 'prefixed'."
                )

            if not requires_pattern and pattern:
                fail(
                    f"Línea {line_number}: cluster no debe "
                    "tener patternType."
                )

            operations = [
                operation.strip()
                for operation in row["operations"].split("|")
                if operation.strip()
            ]

            if not operations:
                fail(
                    f"Línea {line_number}: lista de operaciones vacía."
                )

            if len(operations) != len(set(operations)):
                fail(
                    f"Línea {line_number}: hay operaciones duplicadas."
                )

            allowed = VALID_OPERATIONS_BY_RESOURCE[resource_type]

            invalid = [
                operation
                for operation in operations
                if operation not in allowed
            ]

            if invalid:
                fail(
                    f"Línea {line_number}: operaciones inválidas "
                    f"para {resource_type}: "
                    + ", ".join(invalid)
                )

            row["operations_list"] = operations
            row["line_number"] = line_number
            rows.append(row)

    if not rows:
        fail("El CSV no contiene peticiones.")

    tickets = sorted({row["ticket"] for row in rows})

    if len(tickets) != 1:
        fail(
            "El CSV debe contener un único ticket. Encontrados: "
            + ", ".join(tickets)
        )

    return rows, tickets[0]


def find_or_create_principal(data, principal_name):
    principals = data["principals"]

    for principal in principals:
        if principal.get("name") == principal_name:
            return principal, False

    principal = CommentedMap()
    principal["name"] = principal_name
    principal["permissions"] = CommentedSeq()
    principals.append(principal)

    return principal, True


def find_or_create_resource_list(principal, yaml_key):
    permissions = principal["permissions"]

    for block in permissions:
        if yaml_key in block:
            return block[yaml_key]

    new_block = CommentedMap()
    new_block[yaml_key] = CommentedSeq()
    permissions.append(new_block)

    return new_block[yaml_key]


def find_existing_resource(
    resources,
    resource_type,
    name,
    pattern,
):
    for resource in resources:
        if resource.get("name") != name:
            continue

        if RESOURCE_CONFIG[resource_type]["requires_pattern"]:
            if resource.get("patternType") != pattern:
                continue

        return resource

    return None


def apply_row(data, row):
    principal_name = row["principal"]
    resource_type = row["resourceType"]
    config = RESOURCE_CONFIG[resource_type]
    yaml_key = config["yaml_key"]
    pattern = row["patternType"] or None
    requested_operations = row["operations_list"]

    principal, principal_created = find_or_create_principal(
        data,
        principal_name,
    )

    resources = find_or_create_resource_list(
        principal,
        yaml_key,
    )

    existing = find_existing_resource(
        resources,
        resource_type,
        row["name"],
        pattern,
    )

    if existing is None:
        resource = CommentedMap()
        resource["name"] = row["name"]

        if config["requires_pattern"]:
            resource["patternType"] = pattern

        resource["operations"] = CommentedSeq(
            requested_operations
        )

        resources.append(resource)

        return {
            "status": "added",
            "principal_created": principal_created,
            "principal": principal_name,
            "resource_type": resource_type,
            "name": row["name"],
            "operations": requested_operations,
        }

    current_operations = existing["operations"]
    added_operations = []

    for operation in requested_operations:
        if operation not in current_operations:
            current_operations.append(operation)
            added_operations.append(operation)

    if added_operations:
        status = "merged"
    else:
        status = "unchanged"

    return {
        "status": status,
        "principal_created": principal_created,
        "principal": principal_name,
        "resource_type": resource_type,
        "name": row["name"],
        "operations": added_operations,
    }


def create_backup(yaml_path, backup_dir, ticket):
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_ticket = normalize_ticket_for_branch(ticket)

    backup_path = backup_dir / (
        f"{yaml_path.stem}_{safe_ticket}_{timestamp}"
        f"{yaml_path.suffix}.bak"
    )

    shutil.copy2(yaml_path, backup_path)

    return backup_path


def write_and_revalidate(
    yaml_engine,
    data,
    yaml_path,
):
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(yaml_path.parent),
        prefix=f".{yaml_path.name}.",
        suffix=".tmp",
        delete=False,
    )

    temp_path = Path(temp_file.name)

    try:
        with temp_file:
            yaml_engine.dump(data, temp_file)

        reloaded = load_yaml(yaml_engine, temp_path)
        validate_yaml_model(reloaded)

        os.replace(temp_path, yaml_path)

    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise


def create_git_commit(
    yaml_path,
    ticket,
    branch_name=None,
    commit_message=None,
):
    ensure_git_repository()

    active_branch = run_git(
        ["branch", "--show-current"],
        capture_output=True,
    )

    desired_branch = branch_name or (
        f"acl/{normalize_ticket_for_branch(ticket)}"
    )

    if active_branch != desired_branch:
        existing_branches = run_git(
            ["branch", "--format=%(refname:short)"],
            capture_output=True,
        ).splitlines()

        if desired_branch in existing_branches:
            fail(
                f"La rama {desired_branch} ya existe. "
                "Indica otra con --branch o cámbiate expresamente "
                "a la rama existente."
            )

        run_git(["switch", "-c", desired_branch])

    relative_yaml_path = os.path.relpath(
        yaml_path,
        Path.cwd(),
    )

    run_git(["add", "--", relative_yaml_path])

    staged_diff = subprocess.run(
        ["git", "diff", "--cached", "--quiet", "--", relative_yaml_path]
    )

    if staged_diff.returncode == 0:
        return None, desired_branch

    if staged_diff.returncode != 1:
        fail("No se ha podido revisar el diff preparado para commit.")

    message = commit_message or (
        f"{ticket} - Actualización ACLs Kafka"
    )

    run_git(
        [
            "commit",
            "-m",
            message,
            "--",
            relative_yaml_path,
        ]
    )

    sha = run_git(
        ["rev-parse", "HEAD"],
        capture_output=True,
    )

    return sha, desired_branch


def print_summary(changes):
    print("\nResumen de importación:")

    for change in changes:
        operations = change["operations"]
        operation_text = (
            "|".join(operations)
            if operations
            else "sin cambios"
        )

        print(
            f"  [{change['status'].upper()}] "
            f"{change['principal']} "
            f"{change['resource_type']} "
            f"{change['name']} "
            f"({operation_text})"
        )


def main():
    args = parse_args()

    csv_path = Path(args.csv).resolve()
    yaml_path = Path(args.yaml).resolve()
    backup_dir = Path(args.backup_dir).resolve()

    if not yaml_path.is_file():
        fail(f"No existe el YAML: {yaml_path}")

    yaml_engine = build_yaml()
    data = load_yaml(yaml_engine, yaml_path)

    print("[OK] Sintaxis YAML leída correctamente")

    validate_yaml_model(data)
    print("[OK] Modelo YAML actual validado")

    rows, csv_ticket = read_csv_rows(
        csv_path,
        expected_ticket=args.ticket,
    )

    print(
        f"[OK] CSV validado: {len(rows)} petición/peticiones, "
        f"ticket {csv_ticket}"
    )

    changes = []

    for row in rows:
        changes.append(apply_row(data, row))

    validate_yaml_model(data)
    print("[OK] Resultado en memoria validado")

    print_summary(changes)

    effective_changes = [
        change
        for change in changes
        if change["status"] in {"added", "merged"}
    ]

    if args.check_only:
        print("\n[OK] Check-only completado; no se ha modificado el YAML")
        return 0

    if not effective_changes:
        print(
            "\n[OK] Todas las ACL solicitadas ya existían; "
            "no se modifica el YAML ni se crea commit"
        )
        return 0

    if args.git_commit:
        ensure_git_repository()
        ensure_clean_target_file(yaml_path)

    backup_path = create_backup(
        yaml_path,
        backup_dir,
        csv_ticket,
    )

    print(f"\n[OK] Backup creado: {backup_path}")

    try:
        write_and_revalidate(
            yaml_engine,
            data,
            yaml_path,
        )
    except Exception:
        shutil.copy2(backup_path, yaml_path)
        raise

    print(f"[OK] YAML actualizado y validado: {yaml_path}")

    if args.git_commit:
        try:
            sha, branch = create_git_commit(
                yaml_path=yaml_path,
                ticket=csv_ticket,
                branch_name=args.branch,
                commit_message=args.commit_message,
            )
        except Exception:
            print(
                "[ERROR] El YAML se ha actualizado, pero el commit "
                "Git no se ha completado.",
                file=sys.stderr,
            )
            print(
                f"[INFO] Backup disponible en: {backup_path}",
                file=sys.stderr,
            )
            raise

        if sha:
            print(f"[OK] Rama Git: {branch}")
            print(f"[OK] Commit generado: {sha}")
            print(
                "[INFO] El script no hace push. "
                "Revise el commit antes de publicarlo."
            )
        else:
            print(
                "[OK] Git no detectó diferencias preparadas; "
                "no se creó commit."
            )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ValidationError as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        sys.exit(2)
    except subprocess.CalledProcessError as error:
        print(
            f"[ERROR] Falló el comando: {' '.join(error.cmd)}",
            file=sys.stderr,
        )
        sys.exit(error.returncode or 3)
    except Exception as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        sys.exit(1)
