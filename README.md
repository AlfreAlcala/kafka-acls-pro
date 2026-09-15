# kafka-acls

Repositorio oficial para la gestión de permisos en Kafka Community mediante el modelo **ACLs-as-Code**.

---

# Objetivo

Centralizar la definición de ACLs de Kafka en Git, utilizando YAML como fuente de verdad y scripts automatizados para:

- Gestionar permisos de Topics.
- Gestionar permisos de Consumer Groups.
- Gestionar permisos de Cluster.
- Gestionar permisos de Transactional IDs.
- Facilitar auditoría y rollback.
- Evitar cambios manuales directamente sobre Kafka.

---

# Principios

- Git es la única fuente de verdad.
- Todas las ACLs deben existir en YAML.
- Prohibido crear ACLs manuales en producción.
- Todos los cambios deben quedar asociados a una petición (RFC, Redmine, ServiceNow, etc.).
- Todo cambio debe ser revisado mediante Pull Request.

---

# Flujo ACLs-as-Code

```text
Petición
    ↓
CSV
    ↓
import_csv_to_yaml.py
    ↓
env/pro.yaml
    ↓
Git Commit
    ↓
Pull Request
    ↓
Review
    ↓
apply_acls.py --mode report
    ↓
apply_acls.py --mode apply
    ↓
Verificación
```

---

# Estructura del repositorio

```text
kafka-acls/
├── README.md
├── env/
│   ├── pre.yaml
│   └── pro.yaml
├── peticiones/
├── tools/
│   ├── import_csv_to_yaml.py
│   ├── validate_yaml.py
│   ├── apply_acls.py
│   └── rollback_acl_request.py
└── backup/
```

---

# Recursos soportados

## Topics

```yaml
- topics:
    - name: bus_cpa
      patternType: literal
      operations: [Describe, Read]
```

## Consumer Groups

```yaml
- consumerGroups:
    - name: k8s_bus_cpa_pro
      patternType: literal
      operations: [Describe, Read]
```

## Cluster

```yaml
- clusters:
    - name: kafka-cluster
      operations:
        - Describe
        - IdempotentWrite
```

## Transactional IDs

```yaml
- transactionalIds:
    - name: mm2-
      patternType: prefixed
      operations:
        - Describe
        - Write
```

---

# Tipos de Pattern

## Literal

Aplica únicamente al recurso exacto.

```csv
103097,User:UT12453,topic,bus_cpa,literal,Describe|Read
```

## Prefixed

Aplica a todos los recursos que empiecen por el prefijo indicado.

```csv
103097,User:UT12453,topic,t_networking_,prefixed,Describe|Read
```

Esto cubre recursos como:

```text
t_networking_ise
t_networking_fw_paloalto
t_networking_pam_thycotic
t_networking_umbrella
```

No utilizar:

```text
t_networking_*
```

Utilizar:

```text
name=t_networking_
patternType=prefixed
```

---

# Caso práctico 1: Permisos sobre múltiples topics y consumer groups

## Petición 103097

### Principals

```text
User:UT12453
User:UT12881
User:UM09446
User:UT15839
```

### Topics

```text
t_networking_*
```

### Consumer Groups

```text
dev_networking_consumer*
networking_consumer*
```

## Paso 1. Crear CSV

```bash
cd /root/kafka-acls
vi peticiones/103097.csv
```

### Contenido

```csv
ticket,principal,resourceType,name,patternType,operations
103097,User:UT12453,topic,t_networking_,prefixed,Describe|Read
103097,User:UT12453,consumerGroup,dev_networking_consumer,prefixed,Describe|Read
103097,User:UT12453,consumerGroup,networking_consumer,prefixed,Describe|Read
103097,User:UT12881,topic,t_networking_,prefixed,Describe|Read
103097,User:UT12881,consumerGroup,dev_networking_consumer,prefixed,Describe|Read
103097,User:UT12881,consumerGroup,networking_consumer,prefixed,Describe|Read
```

## Paso 2. Verificar el YAML actual

```bash
grep -i "User:UT12453" env/pro.yaml
```

Si no devuelve resultados:

- Principal nuevo.

Si devuelve resultados:

- Principal existente.

## Paso 3. Validación previa

```bash
python3 tools/import_csv_to_yaml.py \
  --csv peticiones/103097.csv \
  --yaml env/pro.yaml \
  --ticket 103097 \
  --check-only
```

## Paso 4. Aplicar cambios al YAML

```bash
python3 tools/import_csv_to_yaml.py \
  --csv peticiones/103097.csv \
  --yaml env/pro.yaml \
  --ticket 103097
```

## Paso 5. Verificar cambios

```bash
grep -A20 "User:UT12453" env/pro.yaml
```

## Paso 6. Crear commit automáticamente

```bash
python3 tools/import_csv_to_yaml.py \
  --csv peticiones/103097.csv \
  --yaml env/pro.yaml \
  --ticket 103097 \
  --git-commit
```

## Paso 7. Revisar commit

```bash
git show
```

## Paso 8. Publicar rama

```bash
git push -u origin acl/103097
```

---

# Caso práctico 2: Topic y Consumer Group literal

## Petición 86037

### Topic

```text
t_networking_fw_checkpoint
```

### Consumer Groups

```text
connect-splunk_des
connect-splunk_pro
```

## CSV

```csv
ticket,principal,resourceType,name,patternType,operations
86037,User:krb_apigis,topic,t_networking_fw_checkpoint,literal,Describe|Read
86037,User:krb_splunk,topic,t_networking_fw_checkpoint,literal,Describe|Write
86037,User:krb_apigis,consumerGroup,connect-splunk_des,literal,Describe|Read
86037,User:krb_apigis,consumerGroup,connect-splunk_pro,literal,Describe|Read
```

---

# Gestión Git

## Crear rama

```bash
git switch -c acl/103097
```

## Revisar cambios

```bash
git diff
```

## Crear commit

```bash
git commit -m "103097 - Actualización ACLs Kafka"
```

## Publicar rama

```bash
git push -u origin acl/103097
```

---

# Modos de apply_acls.py

## dry-run
No aplica nada en kafka, lista los comnando que aplicara en apply, nosotros aprovechamos para hacer backup de los permisos que se aplicaran

```bash
mkdir -p /opt/kafka_acl_backup/$(date +%Y%m%d_%H%M)
BACKUP_DIR=/opt/kafka_acl_backup/$(date +%Y%m%d_%H%M)
python3 tools/apply_acls.py env/pro.yaml --mode dry-run > $BACKUP_DIR/permisos
```

## report

Analiza diferencias entre Kafka y YAML.

```bash
python3 tools/apply_acls.py env/pro.yaml --mode report
```

## apply

Añade ACLs que faltan. Este es el comando que aplica permisos a Kafka. Aquí es conveniente antes realizar backup de los permisos en kafka:

```bash

 BACKUP_DIR=/opt/kafka_acl_backup/$(date +%Y%m%d_%H%M)
 tools/export_current_acls.sh lxtmbkafdes01.xarxa.interna:9093 /etc/kafka/admin.properties > $BACKUP_DIR/acls_before.txt

```

```bash

python3 tools/apply_acls.py env/pro.yaml \
  --mode apply \
  --execute
```

## reconcile

Añade ACLs faltantes y elimina ACLs sobrantes.

```bash
python3 tools/apply_acls.py env/pro.yaml \
  --mode reconcile \
  --execute
```

---

# ACLs protegidas

Las ACLs de infraestructura no deben eliminarse automáticamente.

```yaml
reconcile:
  protectedPrincipals:
    - User:kafka
    - User:mmk_conf_pro
    - User:akhq
```

Por defecto el borrado de ACLs de tipo Cluster está bloqueado.

---

# Ejemplos Cluster

```csv
ticket,principal,resourceType,name,patternType,operations
RFC456789,User:upe02423,cluster,kafka-cluster,,IdempotentWrite
```

Resultado:

```yaml
- clusters:
    - name: kafka-cluster
      operations:
        - IdempotentWrite
```

---

# Ejemplos Transactional IDs

```csv
ticket,principal,resourceType,name,patternType,operations
RFC777777,User:mmk_conf_pro,transactionalId,mm2-,prefixed,Describe|Write
```

---

# Procedimiento recomendado para Producción

1. Crear CSV.
2. Ejecutar validación (--check-only).
3. Modificar YAML.
4. Crear commit Git.
5. Pull Request.
6. Revisión técnica.
7. Ejecutar report.
8. Backup ACLs actuales.
9. Ejecutar apply o reconcile.
10. Verificación final.
11. Adjuntar evidencias.

---

# Rollback

## Escenario 1. Error durante la importación

Restaurar backup:

```bash
cp backup/pro_rfc123456_20260908_103000.yaml.bak env/pro.yaml
```

Validar:

```bash
python3 tools/validate_yaml.py env/pro.yaml
```

## Escenario 2. Antes del merge

```bash
git switch main
git branch -D acl/rfc123456
```

## Escenario 3. Después del merge

```bash
git revert <sha>
git push
```

Reaplicar:

```bash
python3 tools/apply_acls.py env/pro.yaml --mode apply --execute
```

## Escenario 4. Error en Kafka

Backup previo:

```bash
kafka-acls \
  --bootstrap-server lxtmbkafpro01.xarxa.interna:9093 \
  --command-config /etc/kafka/admin.properties \
  --list > backup/acls_before.txt
```

Restaurar YAML:

```bash
cp backup/pro_antes_rfc123456.yaml env/pro.yaml
```

Reaplicar:

```bash
python3 tools/apply_acls.py env/pro.yaml --mode reconcile --execute
```

---

# Troubleshooting

## GROUP_AUTHORIZATION_FAILED

Comprobar permisos de Consumer Group.

## TOPIC_AUTHORIZATION_FAILED

Comprobar permisos de Topic.

## El principal no consume

Verificar:

- Topic ACLs.
- Consumer Group ACLs.
- Principal Kerberos.
- Ticket Kerberos.

## Diferencias entre Kafka y YAML

```bash
python3 tools/apply_acls.py env/pro.yaml --mode report
```

---

# Conclusión

Kafka ACLs-as-Code permite gestionar permisos de forma auditable, repetible y reversible, utilizando Git como fuente de verdad y YAML como definición declarativa de seguridad.
