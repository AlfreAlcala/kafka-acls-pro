# kafka-acls
Repositorio oficial de ACLs Kafka Community (ACLs-as-Code).

## Principios
- Git es la única fuente de verdad.
- Prohibido aplicar ACLs manuales sin PR.

## Flujo
1) Edita `env/dev.yaml` (o pre/prod)
2) Revisa el diff en PR
3) Aplica con `tools/apply_acls.py`

## Nota importante
Kafka Community no soporta grupos LDAP. Las políticas Ranger se traducen a principals individuales.
`identity_map.yaml` mapea *roles/grupos Ranger* -> *principals Kafka*.

python3 tools/apply_acls.py env/pro.yaml --mode apply
tools/export_current_acls.sh lxtmbkafdes01.xarxa.interna:9093 /etc/kafka/admin.properties | less

kafka-acls --bootstrap-server lxtmbkafdes01.xarxa.interna:9093 --command-config /etc/kafka/admin.properties --add --allow-principal User:upe02423 --operation Read --topic bus_cpa
