import argparse, subprocess, re
from pathlib import Path
import yaml

PATTERN_FLAG = {"literal": "literal", "prefixed": "prefixed"}

RESOURCE_FLAG = {
  "topics": "--topic",
  "consumerGroups": "--group",
  "clusters": "--cluster",
  "transactionalIds": "--transactional-id",
}

RESOURCE_TYPE_MAP = {
  "topics": "TOPIC",
  "consumerGroups": "GROUP",
  "clusters": "CLUSTER",
  "transactionalIds": "TRANSACTIONAL_ID",
}

OP_ALLOWED = {
  "Read","Write","Describe","Create","Delete","Alter",
  "DescribeConfigs","AlterConfigs",
  "ClusterAction","IdempotentWrite","All",
  "DescribeTokens","CreateTokens","TwoPhaseCommit"
}

RE_RESOURCE = re.compile(
  r"ResourcePattern\(resourceType=(?P<rtype>[^,]+),\s*name=(?P<rname>[^,]+),\s*patternType=(?P<ptype>[^)]+)\)",
  re.IGNORECASE
)
RE_ENTRY = re.compile(
  r"\(principal=(?P<principal>[^,]+),\s*host=(?P<host>[^,]+),\s*operation=(?P<op>[^,]+),\s*permissionType=(?P<perm>[^)]+)\)",
  re.IGNORECASE
)

def run(cmd, dry):
    print(" ".join(cmd))
    if dry:
        return 0
    return subprocess.run(cmd).returncode

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
      perm.strip().upper()
    )

def load_existing_acls(bootstrap, command_cfg):
    out = run_capture(["kafka-acls","--bootstrap-server",bootstrap,"--command-config",command_cfg,"--list"])
    existing = set()
    current_resource = None
    for line in out.splitlines():
        mres = RE_RESOURCE.search(line)
        if mres:
            current_resource = (mres.group("rtype"), mres.group("rname"), mres.group("ptype"))
            continue
        ment = RE_ENTRY.search(line)
        if ment and current_resource:
            rtype, rname, ptype = current_resource
            existing.add(acl_key(rtype, rname, ptype,
                                 ment.group("principal"), ment.group("host"),
                                 ment.group("op"), ment.group("perm")))
    return existing

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("env_yaml", help="env/dev.yaml")
    ap.add_argument("--mode", choices=["apply","dry-run"], default="dry-run")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.env_yaml).read_text())
    cluster = cfg["cluster"]
    bootstrap = cluster["bootstrap"]
    command_cfg = cluster["command_config"]
    dry = (args.mode != "apply") or bool(cluster.get("dry_run", False))

    existing = set()
    if not dry:
        existing = load_existing_acls(bootstrap, command_cfg)

    for p in cfg.get("principals", []):
        principal = p["name"]
        for perm in p.get("permissions", []):
            for section, flag in RESOURCE_FLAG.items():
                for entry in perm.get(section, []) or []:
                    name = entry["name"]
                    ptype = entry.get("patternType", "literal").lower()

                    if ptype == "wildcard":
                        raise SystemExit("patternType 'wildcard' no soportado: usar name='*' con patternType 'literal'")
                    if ptype not in ("literal","prefixed"):
                        raise SystemExit(f"patternType no soportado: {ptype}")

                    ops = entry.get("operations", [])
                    for op in ops:
                        if op not in OP_ALLOWED:
                            raise SystemExit(f"Operacion no soportada: {op}")

                        # SKIP si ya existe (solo en apply)
                        if not dry:
                            rtype = RESOURCE_TYPE_MAP.get(section, section.upper())
                            cand = acl_key(rtype, name, ptype, principal, "*", op, "ALLOW")
                            if cand in existing:
                                print(f"SKIP (ya existe): {principal} {op} {rtype}:{name} ({ptype})")
                                continue

                        cmd = [
                          "kafka-acls",
                          "--bootstrap-server", bootstrap,
                          "--command-config", command_cfg,
                          "--add",
                          "--allow-principal", principal,
                          "--operation", op,
                          flag, name,
                        ]
                        # Solo añadir resource-pattern-type si es prefixed (para literal es el default)
                        if ptype == "prefixed":
                            cmd += ["--resource-pattern-type", "prefixed"]

                        rc = run(cmd, dry)
                        if rc != 0:
                            raise SystemExit(rc)

                        if not dry:
                            rtype = RESOURCE_TYPE_MAP.get(section, section.upper())
                            existing.add(acl_key(rtype, name, ptype, principal, "*", op, "ALLOW"))

if __name__ == "__main__":
    main()
