#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path
import yaml

# Operaciones típicas vistas en kafka-acls --help (y compatibles con Kafka/Confluent)
OP_ALLOWED = {
    "Alter", "AlterConfigs", "ClusterAction", "Create", "CreateTokens",
    "Delete", "Describe", "DescribeConfigs", "DescribeTokens",
    "IdempotentWrite", "Read", "TwoPhaseCommit", "Write", "All",
}

PATTERN_ALLOWED = {"literal", "prefixed"}  # MATCH/ANY no se usan en --add

SECTIONS = {"topics", "consumerGroups", "clusters", "transactionalIds"}

def eprint(*a):
    print(*a, file=sys.stderr)

def load_yaml(path: Path):
    try:
        return yaml.safe_load(path.read_text())
    except Exception as ex:
        raise SystemExit(f"[ERROR] YAML inválido: {path}\n{ex}")

def norm_principal(p: str):
    # Mantén "User:" tal cual y normaliza la parte de identidad
    # Si no empieza por User:, igualmente lo normalizamos para comparar.
    p = (p or "").strip()
    if ":" in p:
        ptype, rest = p.split(":", 1)
        return (ptype.strip(), rest.strip())
    return ("", p)

def principal_ci_key(p: str):
    ptype, rest = norm_principal(p)
    return (ptype.strip().lower(), rest.strip().lower())

def validate_root(doc):
    errors = []
    if not isinstance(doc, dict):
        return ["Documento YAML raíz debe ser un dict/map."]
    if "cluster" not in doc or not isinstance(doc["cluster"], dict):
        errors.append("Falta cluster: {...} o no es un dict.")
    if "principals" not in doc or not isinstance(doc["principals"], list):
        errors.append("Falta principals: [...] o no es una lista.")
    return errors

def validate_cluster(cluster):
    errors = []
    for k in ("name", "bootstrap", "command_config"):
        if k not in cluster or not str(cluster.get(k, "")).strip():
            errors.append(f"cluster.{k} es obligatorio y no puede estar vacío.")
    return errors

def validate_principal_obj(pobj, idx):
    errors = []
    warnings = []
    if not isinstance(pobj, dict):
        return [f"principals[{idx}] no es un dict."], []
    name = pobj.get("name")
    if not isinstance(name, str) or not name.strip():
        errors.append(f"principals[{idx}].name es obligatorio (string).")
    perms = pobj.get("permissions", [])
    if not isinstance(perms, list):
        errors.append(f"principals[{idx}].permissions debe ser lista.")
    return errors, warnings

def lint(path: Path, strict_case=False, write_fixed=False):
    doc = load_yaml(path)
    errors = []
    warnings = []

    errors += validate_root(doc)
    if errors:
        return errors, warnings, None

    cluster = doc["cluster"]
    errors += validate_cluster(cluster)

    principals = doc.get("principals", [])
    # 1) Detectar duplicados por case-insensitive de principal
    seen_ci = {}
    for i, pobj in enumerate(principals):
        pe, pw = validate_principal_obj(pobj, i)
        errors += pe
        warnings += pw
        if pe:
            continue
        pname = pobj["name"].strip()
        key = principal_ci_key(pname)
        if key in seen_ci:
            msg = (f"Principal duplicado por case-insensitive: '{pname}' "
                   f"vs '{seen_ci[key]}' (Kafka compara strings; revisa el principal real).")
            if strict_case:
                errors.append(msg)
            else:
                warnings.append(msg)
        else:
            seen_ci[key] = pname

    # 2) Detectar redundancias dentro de cada principal
    #    Merge interno: mismo recurso + patternType => unir operaciones
    fixed_doc = {"cluster": cluster, "principals": []}

    for i, pobj in enumerate(principals):
        if not isinstance(pobj, dict) or "name" not in pobj:
            continue
        pname = pobj["name"].strip()
        perms = pobj.get("permissions", [])
        if not isinstance(perms, list):
            continue

        # resource_map: (section, name, patternType) -> set(ops)
        resource_map = {}
        # Track duplicates (para reportar)
        duplicates = set()

        for j, perm in enumerate(perms):
            if not isinstance(perm, dict):
                errors.append(f"{pname}: permissions[{j}] no es dict.")
                continue

            # Unknown keys inside permission block
            unknown = set(perm.keys()) - SECTIONS
            if unknown:
                warnings.append(f"{pname}: keys desconocidas en permissions[{j}]: {sorted(unknown)}")

            for section in SECTIONS:
                items = perm.get(section, []) or []
                if section == "clusters":
                    # clusters suele ser lista de dicts con name + operations
                    if not items:
                        continue
                    if not isinstance(items, list):
                        errors.append(f"{pname}: '{section}' debe ser lista.")
                        continue
                    for k, entry in enumerate(items):
                        if not isinstance(entry, dict):
                            errors.append(f"{pname}: {section}[{k}] no es dict.")
                            continue
                        cname = str(entry.get("name", "kafka-cluster")).strip() or "kafka-cluster"
                        ops = entry.get("operations", [])
                        if not isinstance(ops, list):
                            errors.append(f"{pname}: clusters[{k}].operations debe ser lista.")
                            continue
                        # En clusters no hay patternType
                        rkey = (section, cname, "literal")
                        sops = resource_map.setdefault(rkey, set())
                        before = set(sops)
                        for op in ops:
                            if op not in OP_ALLOWED:
                                errors.append(f"{pname}: operación no soportada '{op}' en clusters[{k}].")
                            sops.add(op)
                        if before & set(ops):
                            duplicates.add(rkey)
                    continue

                # Resto: topics / consumerGroups / transactionalIds
                if not items:
                    continue
                if not isinstance(items, list):
                    errors.append(f"{pname}: '{section}' debe ser lista.")
                    continue

                for k, entry in enumerate(items):
                    if not isinstance(entry, dict):
                        errors.append(f"{pname}: {section}[{k}] no es dict.")
                        continue
                    rname = str(entry.get("name", "")).strip()
                    if not rname:
                        errors.append(f"{pname}: {section}[{k}].name vacío.")
                        continue
                    ptype = str(entry.get("patternType", "literal")).strip().lower()
                    if ptype not in PATTERN_ALLOWED:
                        errors.append(f"{pname}: {section}[{k}].patternType inválido '{ptype}' (solo literal/prefixed).")
                        continue
                    ops = entry.get("operations", [])
                    if not isinstance(ops, list):
                        errors.append(f"{pname}: {section}[{k}].operations debe ser lista.")
                        continue

                    # Reglas anti-“wildcard raro”
                    if rname == "*" and ptype != "literal":
                        warnings.append(f"{pname}: {section}[{k}] usa name='*' con patternType='{ptype}'. "
                                        f"Recomendado: literal (wildcard por nombre).")

                    rkey = (section, rname, ptype)
                    sops = resource_map.setdefault(rkey, set())
                    before = set(sops)
                    for op in ops:
                        if op not in OP_ALLOWED:
                            errors.append(f"{pname}: operación no soportada '{op}' en {section}[{k}] '{rname}'.")
                        sops.add(op)
                    if before & set(ops):
                        duplicates.add(rkey)

        # Reportar duplicados/redundancias detectadas
        for (section, rname, ptype) in sorted(duplicates):
            warnings.append(f"{pname}: redundancia detectada (operación repetida o recurso repetido) "
                            f"en {section} '{rname}' ({ptype}). Se recomienda consolidar.")

        # Construir versión “fixed/merged” del principal
        merged_perms = {}

        # Reagrupar por sección
        for (section, rname, ptype), opset in resource_map.items():
            merged_perms.setdefault(section, [])
            if section == "clusters":
                merged_perms[section].append({"name": rname, "operations": sorted(opset)})
            else:
                merged_perms[section].append({"name": rname, "patternType": ptype, "operations": sorted(opset)})

        # Orden estable (opcional)
        for sec in merged_perms:
            merged_perms[sec] = sorted(
                merged_perms[sec],
                key=lambda x: (x.get("name",""), x.get("patternType",""))
            )

        fixed_doc["principals"].append({
            "name": pname,
            "permissions": [merged_perms] if merged_perms else []
        })

    fixed_yaml = None
    if write_fixed:
        fixed_yaml = yaml.safe_dump(fixed_doc, sort_keys=False, allow_unicode=True)

    return errors, warnings, fixed_yaml

def main():
    ap = argparse.ArgumentParser(description="Lint de ACLs-as-Code para Kafka (evita duplicados y redundancias).")
    ap.add_argument("yaml_file", help="env/dev.yaml")
    ap.add_argument("--strict-case", action="store_true",
                    help="Trata duplicados case-insensitive de principals como ERROR (default: WARNING).")
    ap.add_argument("--write-fixed", action="store_true",
                    help="Imprime una versión merged/clean del YAML (sin duplicados).")
    args = ap.parse_args()

    path = Path(args.yaml_file)
    if not path.exists():
        raise SystemExit(f"[ERROR] No existe: {path}")

    errors, warnings, fixed = lint(path, strict_case=args.strict_case, write_fixed=args.write_fixed)

    for w in warnings:
        eprint("[WARN]", w)
    for e in errors:
        eprint("[ERROR]", e)

    if args.write_fixed and fixed is not None:
        print(fixed)

    # Exit codes
    if errors:
        sys.exit(2)
    elif warnings:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
