# kafka-acls
Repositorio oficial de ACLs Kafka Community (ACLs-as-Code).

# Principios
- Git es la única fuente de verdad.
- Prohibido aplicar ACLs manuales sin PR.

# Flujo
1) Edita `env/pro.yaml` (o pre/prod)
2) Revisa el diff en PR
3) Aplica con `tools/apply_acls.py`

# Nota importante
Kafka Community no soporta grupos LDAP. Las políticas Ranger se traducen a principals individuales.
`identity_map.yaml` mapea *roles/grupos Ranger* -> *principals Kafka*.

python3 tools/apply_acls.py env/pro.yaml --mode apply
tools/export_current_acls.sh lxtmbkafdes01.xarxa.interna:9093 /etc/kafka/admin.properties | less

kafka-acls --bootstrap-server lxtmbkafdes01.xarxa.interna:9093 --command-config /etc/kafka/admin.properties --add --allow-principal User:upe02423 --operation Read --topic bus_cpa

# Flujo ACLs-as-Code en Kafka Community

Peticion → CSV → Commit → Push → Review → Deploy ACLs

# Caso práctico utilizando patterns para varios topics o consumergroups.
Supongamos una petición:103097

  ## Principals:
  Users:UT12453, UT12881, UM09446, UT15839 

  ## Permisos:
 ### Topic:
     Topics con nombre t_networking_*
 ### Consumer Groups:
     dev_networking_consumer*, networking_consumer*

# Paso 1. Crear CSV. Ejemplo que parte de petición en Redmine
Path en nodo lxtmbkafpro01: /root/kafka-acls
cd  /root/kafka-acls
git init
vi peticiones/103097.csv

## Contenido:

ticket,principal,resourceType,name,patternType,operations
103097,User:UT12453,topic,t_networking_,prefixed,Describe|Read
103097,User:UT12453,consumerGroup,dev_networking_consumer,prefixed,Describe|Read
103097,User:UT12453,consumerGroup,networking_consumer,prefixed,Describe|Read
103097,User:UT12881,topic,t_networking_,prefixed,Describe|Read
103097,User:UT12881,consumerGroup,dev_networking_consumer,prefixed,Describe|Read
103097,User:UT12881,consumerGroup,networking_consumer,prefixed,Describe|Read
103097,User:UT15038,topic,t_networking_,prefixed,Describe|Read
103097,User:UT15038,consumerGroup,dev_networking_consumer,prefixed,Describe|Read
103097,User:UT15038,consumerGroup,networking_consumer,prefixed,Describe|Read
103097,User:UM09446,topic,t_networking_,prefixed,Describe|Read
103097,User:UM09446,consumerGroup,dev_networking_consumer,prefixed,Describe|Read
103097,User:UM09446,consumerGroup,networking_consumer,prefixed,Describe|Read
103097,User:UM09524,topic,t_networking_,prefixed,Describe|Read
103097,User:UM09524,consumerGroup,dev_networking_consumer,prefixed,Describe|Read
103097,User:UM09524,consumerGroup,networking_consumer,prefixed,Describe|Read
103097,User:UT15839,topic,t_networking_,prefixed,Describe|Read
103097,User:UT15839,consumerGroup,dev_networking_consumer,prefixed,Describe|Read
103097,User:UT15839,consumerGroup,networking_consumer,prefixed,Describe|Read

## Paso 2. Verificar el YAML actual

grep -i "User:UT12453" env/pro.yaml

Si no devuelve nada:
Principal nuevo
Si devuelve resultados:
Principal existente

## Paso 3. Validación previa (modo check)
Ejecutar:

python3 tools/import_csv_to_yaml.py --csv peticiones/103097.csv /
--yaml env/pro.yaml --ticket 103097 --check-only

## Salida esperada:

[OK] Sintaxis YAML leída correctamente
[OK] Modelo YAML actual validado
[OK] CSV validado: 18 petición/peticiones, ticket 103097
[OK] Resultado en memoria validado

Resumen de importación:
  [ADDED] User:UT12453 topic t_networking_ (Describe|Read)
  [ADDED] User:UT12453 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UT12453 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UT12881 topic t_networking_ (Describe|Read)
  [ADDED] User:UT12881 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UT12881 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UT15038 topic t_networking_ (Describe|Read)
  [ADDED] User:UT15038 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UT15038 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UM09446 topic t_networking_ (Describe|Read)
  [ADDED] User:UM09446 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UM09446 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UM09524 topic t_networking_ (Describe|Read)
  [ADDED] User:UM09524 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UM09524 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UT15839 topic t_networking_ (Describe|Read)
  [ADDED] User:UT15839 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UT15839 consumerGroup networking_consumer (Describe|Read)

[OK] Check-only completado; no se ha modificado el YAML

## Paso 4. Aplicar cambios al YAML
Cuando la revisión sea correcta:
### python3 tools/import_csv_to_yaml.py --csv peticiones/103097.csv --yaml env/pro.yaml --ticket 103097
El proceso creara Backup del yaml en /opt/kafka/backup/

# Salida:

 [OK] Sintaxis YAML leída correctamente
[OK] Modelo YAML actual validado
[OK] CSV validado: 18 petición/peticiones, ticket 103097
[OK] Resultado en memoria validado

Resumen de importación:
  [ADDED] User:UT12453 topic t_networking_ (Describe|Read)
  [ADDED] User:UT12453 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UT12453 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UT12881 topic t_networking_ (Describe|Read)
  [ADDED] User:UT12881 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UT12881 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UT15038 topic t_networking_ (Describe|Read)
  [ADDED] User:UT15038 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UT15038 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UM09446 topic t_networking_ (Describe|Read)
  [ADDED] User:UM09446 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UM09446 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UM09524 topic t_networking_ (Describe|Read)
  [ADDED] User:UM09524 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UM09524 consumerGroup networking_consumer (Describe|Read)
  [ADDED] User:UT15839 topic t_networking_ (Describe|Read)
  [ADDED] User:UT15839 consumerGroup dev_networking_consumer (Describe|Read)
  [ADDED] User:UT15839 consumerGroup networking_consumer (Describe|Read)

[OK] Backup creado: /opt/kafka/backup/pro_103097_20260909_104654.yaml.bak
[OK] YAML actualizado y validado: /root/kafka-acls/env/pro.yaml

## Paso 5. Verificar el cambio
Buscar el principal:

grep -A20 "User:UT12453" env/pro.yaml
  - name: User:UT12453
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
          - name: networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
  - name: User:UT12881
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
grep -A20 "User:UT12881" env/pro.yaml
  - name: User:UT12881
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
          - name: networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
  - name: User:UT15038
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
grep -A20 "User:UT15038" env/pro.yaml
  - name: User:UT15038
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
          - name: networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
  - name: User:UM09446
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
grep -A20 "User:UM09446" env/pro.yaml
  - name: User:UM09446
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
          - name: networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
  - name: User:UM09524
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
grep -A20 "User:UM09524" env/pro.yaml
  - name: User:UM09524
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
          - name: networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
  - name: User:UT15839
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
grep -A20 "User:UT15839" env/pro.yaml
  - name: User:UT15839
    permissions:
      - topics:
          - name: t_networking_
            patternType: prefixed
            operations: [Describe, Read]
      - consumerGroups:
          - name: dev_networking_consumer
            patternType: prefixed
            operations: [Describe, Read]
          - name: networking_consumer
            patternType: prefixed
            operations: [Describe, Read]

## Paso 6. Crear commit automáticamente.
Si el repositorio Git ya está inicializado, lo inicializamos en el paso 1:
python3 tools/import_csv_to_yaml.py --csv peticiones/103097.csv / 
--yaml env/pro.yaml --ticket 103097 --git-commit

## El script realizará:
git switch -c acl/103097
git add .
git commit "103097 - Actualización ACLs Kafka"

## Paso 7. Revisar el commit
Ver qué ha cambiado:
git show
o
git diff main

## Paso 8. Publicar rama
git push -u origin acl/103097

## Paso 9. Una vez aprobado
 Validar ACLs Kafka:
### python3 tools/apply_acls.py env/pro.yaml --mode dry-run

Revisar resultado.

## Paso 10. Aplicación en Kafka

Si el dry-run es correcto:
python3 tools/apply_acls.py env/pro.yaml --mode apply

## Caso práctico utilizando literal para aplicar a topic o consumergroup.

Path en nodo lxtmbkafpro01: /root/kafka-acls
cd /root/kafka-acls
git init

Supongamos una petición: 86037
otorgar permisos de lectura a los consumers: connect-splunk_des, connect-splunk_pro
otorgar permisos de lectura a topic: t_networking_fw_checkpoint

Principal:
Users:krb_apigis, Permisos: Describe, Read
User: krb_splunk, permisos: Describe, Write

Topic: t_networking_fw_checkpoint
Consumer Groups: splunk_pro, splunk_des, Permisos: Describe, Read

## Paso 1. Crear CSV. Ejemplo que parte de petición en Redmine

vi peticiones/86037.csv

Contenido:
ticket,principal,resourceType,name,patternType,operations
86037,User:krb_apigis,topic,t_networking_fw_checkpoint,literal,Describe|Read
86037,User:krb_splunk,topic,t_networking_fw_checkpoint,literal,Describe|Write
86037,User:krb_apigis,consumerGroup,splunk_pro,literal,Describe|Read
86037,User:krb_apigis,consumerGroup,splunk_des,literal,Describe|Read

## Paso 2. Verificar el YAML actual

[root@lxtmbkafpro01 kafka-acls]# grep -i "User:krb_apigis\|User:krb_splunk" env/pro.yaml

Si no devuelve nada:
Principal nuevo
Si devuelve resultados:
Principal existente, nuestro caso.

## Paso 3. Validación previa (modo check)
Ejecutar:

python3 tools/import_csv_to_yaml.py --csv peticiones/86037.csv --yaml env/pro.yaml --ticket 86037 --check-only

### Salida esperada:

[OK] Sintaxis YAML leída correctamente
[OK] Modelo YAML actual validado
[OK] CSV validado: 9 petición/peticiones, ticket 86037
[OK] Resultado en memoria validado

Resumen de importación:
  [ADDED] User:KRB_ELK topic t_networking_fw_checkpoint (Describe|Read)
  [ADDED] User:KRB_TEST topic t_networking_fw_checkpoint (Describe|Read)
  [ADDED] User:KRB_SPLUNK topic t_networking_fw_checkpoint (Describe|Read)
  [ADDED] User:KRB_ELK consumerGroup connect-splunk_des (Describe|Read)
  [ADDED] User:KRB_TEST consumerGroup connect-splunk_des (Describe|Read)
  [ADDED] User:KRB_SPLUNK consumerGroup connect-splunk_des (Describe|Read)
  [ADDED] User:KRB_ELK consumerGroup connect-splunk_pro (Describe|Read)
  [ADDED] User:KRB_TEST consumerGroup connect-splunk_pro (Describe|Read)
  [ADDED] User:KRB_SPLUNK consumerGroup connect-splunk_pro (Describe|Read)

[OK] Check-only completado; no se ha modificado el YAML

## Paso 4. Aplicar cambios al YAML
Cuando la revisión sea correcta:
Si el repositorio Git ya está inicializado, ejecutado en paso 1:

### python3 tools/import_csv_to_yaml.py --csv peticiones/86037.csv / 
### --yaml env/pro.yaml --ticket 86037 --git-commit



### El script realizará:

Backup creado en  /opt/kafka/backup/
git switch -c acl/86037
git add .
git commit "86037 - Actualización ACLs Kafka"
python3 tools/import_csv_to_yaml.py --csv peticiones/86037.csv /
--yaml env/pro.yaml --ticket 86037 --git-commit

[root@lxtmbkafpro01 kafka-acls]# python3 tools/import_csv_to_yaml.py --csv peticiones/86037.csv --yaml env/pro.yaml --ticket 86037 --git-commit
[OK] Sintaxis YAML leída correctamente
[OK] Modelo YAML actual validado
[OK] CSV validado: 9 petición/peticiones, ticket 86037
[OK] Resultado en memoria validado

Resumen de importación:
  [ADDED] User:KRB_ELK topic t_networking_fw_checkpoint (Describe|Read)
  [ADDED] User:KRB_TEST topic t_networking_fw_checkpoint (Describe|Read)
  [ADDED] User:KRB_SPLUNK topic t_networking_fw_checkpoint (Describe|Read)
  [ADDED] User:KRB_ELK consumerGroup connect-splunk_des (Describe|Read)
  [ADDED] User:KRB_TEST consumerGroup connect-splunk_des (Describe|Read)
  [ADDED] User:KRB_SPLUNK consumerGroup connect-splunk_des (Describe|Read)
  [ADDED] User:KRB_ELK consumerGroup connect-splunk_pro (Describe|Read)
  [ADDED] User:KRB_TEST consumerGroup connect-splunk_pro (Describe|Read)
  [ADDED] User:KRB_SPLUNK consumerGroup connect-splunk_pro (Describe|Read)

[OK] Backup creado: /opt/kafka/backup/pro_86037_20260910_144151.yaml.bak
[OK] YAML actualizado y validado: /root/kafka-acls/env/pro.yaml
Switched to a new branch 'acl/86037'
[acl/86037 8c72378] 86037 - Actualización ACLs Kafka
 Committer: root <root@lxtmbkafpro01.xarxa.interna>
Your name and email address were configured automatically based
on your username and hostname. Please check that they are accurate.
You can suppress this message by setting them explicitly:

    git config --global user.name "Your Name"
    git config --global user.email you@example.com

After doing this, you may fix the identity used for this commit with:

    git commit --amend --reset-author

 1 file changed, 36 insertions(+)
[OK] Rama Git: acl/86037
[OK] Commit generado: 8c72378cffd62bb8bed90843321ecdc4560b76d9
[INFO] El script no hace push. Revise el commit antes de publicarlo.

## Ejecutamos: git push --set-upstream origin acl/86037

## Paso 5. Verificar el cambio
Buscar el principal:

grep -A20 "User:krb_api_gis\|User:krb_splunk" env/pro.yaml
    - name: User:KRB_ELK
    permissions:
      - topics:
          - name: t_syslogs
            patternType: literal
            operations: [Describe, Read]
          - name: t_eventlogs
            patternType: literal
            operations: [Describe, Read]
          - name: t_networking_fw_checkpoint
            patternType: literal
            operations: [Describe, Read]

      - consumerGroups:
          - name: connect-splunk_des
            patternType: literal
            operations: [Describe, Read]
          - name: connect-splunk_pro
            patternType: literal
            operations: [Describe, Read]
  - name: User:KRB_SYSLOG
--
  - name: User:KRB_TEST
    permissions:
      - topics:
          - name: t_networking_fw_checkpoint
            patternType: literal
            operations: [Describe, Read]
      - consumerGroups:
          - name: connect-splunk_des
            patternType: literal
            operations: [Describe, Read]
          - name: connect-splunk_pro
            patternType: literal
            operations: [Describe, Read]
  - name: User:KRB_SPLUNK
    permissions:
      - topics:
          - name: t_networking_fw_checkpoint
            patternType: literal
            operations: [Describe, Read]
      - consumerGroups:
          - name: connect-splunk_des
            patternType: literal
            operations: [Describe, Read]
          - name: connect-splunk_pro
            patternType: literal
            operations: [Describe, Read]

## Paso 6. Revisar el commit
Ver qué ha cambiado:
git show
o
git diff main


## Paso 8. Una vez aprobado
 Validar ACLs Kafka:
 
### python3 tools/apply_acls.py env/pro.yaml --mode dry-run

Revisar resultado.

## Paso 9. Aplicación en Kafka
Si el dry-run es correcto:

### python3 tools/apply_acls.py env/pro.yaml --mode apply

Ejemplo con permisos de Cluster
Petición:
RFC456789

Principal:
User:upe02423

Necesita:
IdempotentWrite
sobre Cluster

CSV:
ticket,principal,resourceType,name,patternType,operations
RFC456789,User:upe02423,cluster,kafka-cluster,,IdempotentWrite
Ejecucion:
python3 tools/import_csv_to_yaml.py \
  --csv solicitudes/RFC456789.csv \
  --yaml env/pro.yaml \
  --ticket RFC456789
Resultado:
- clusters:
    - name: kafka-cluster
      operations:
        - IdempotentWrite

## Ejemplo con Transactional IDs (MirrorMaker2)
Muy útil para vuestro mmk_conf_pro.
CSV:
ticket,principal,resourceType,name,patternType,operations
RFC777777,User:mmk_conf_pro,transactionalId,mm2-,prefixed,Describe|Write
Resultado:
- transactionalIds:
    - name: mm2-
      patternType: prefixed
      operations:
        - Describe
        - Write
## Procedimiento recomendado para Producción

Para env/pro.yaml en los nodos KRaft de Producción:
1. Crear CSV RFC
2. import_csv_to_yaml.py --check-only
3. import_csv_to_yaml.py --git-commit
4. Pull Request
5. Revisión
6. apply_acls.py --mode dry-run
7. Backup ACL actual
8. apply_acls.py --mode apply
9. Export ACL final
10. Evidencias

## ROOLBACKS

El rollback debe contemplar dos Escenarios distintos:
- Error al importar/modificar el YAML.
- Error después de aplicar ACLs en Kafka.
La documentación interna ya establece que Git es la fuente de verdad y aporta versionado, auditoría y rollback, mientras que Kafka no debe considerarse fuente de verdad.

### Escenario 1. Error durante la importación del CSV
Este es el caso más sencillo.
El script que te propuse genera previamente:

backup/
└── pro_rfc123456_20260908_103000.yaml.bak

Si se detectas que el YAML generado es incorrecto:

cp backup/pro_rfc123456_20260908_103000.yaml.bak env/pro.yaml

Validar:

python3 validate_yaml.py env/pro.yaml

Revisar cambios:

git diff

### Escenario 2. Rollback antes del merge
Si ya tienes commit pero todavía no has hecho merge en main:
Ver rama:

git branch

Eliminar la rama:

git switch main
 
git branch -D acl/rfc123456

o simplemente cerrar el PR.
No afecta a Kafka porque todavía no se ha ejecutado:

apply_acls.py

### Escenario 3. Rollback después del merge
Supongamos:

Commit A
↓
Commit B (RFC123456)
↓
Problemas

Identificar SHA:

git log --oneline

Ejemplo:

87acde1 RFC123456 - ACL update
45ef911 Commit anterior

Crear reversión:

git revert 87acde1

Generará un nuevo commit:

9bd1234 Revert RFC123456 - ACL update

Después:

git push

y aplicar nuevamente:

python3 tools/apply_acls.py \
env/pro.yaml \
--mode apply

Este es el rollback más limpio porque mantiene la trazabilidad Git.

### Escenario 4. Error tras aplicar ACLs en Kafka
Éste es el importante para Producción.
Por eso el procedimiento debe incluir siempre:
Backup ACLs antes

kafka-acls --bootstrap-server lxtmbkafpro01.xarxa.interna:9093 --command-config /etc/kafka/admin.properties --list > backup/acls_before.txt

Backup YAML antes

cp env/pro.yaml backup/pro_antes_rfc123456.yaml

### Rollback rápido en Kafka
Si una ACL nueva provoca:

TOPIC_AUTHORIZATION_FAILED
o
GROUP_AUTHORIZATION_FAILED

volver al YAML anterior:

cp backup/pro_antes_rfc123456.yaml env/pro.yaml

y reaplicar:

python3 tools/apply_acls.py env/pro.yaml --mode apply

### Escenario 5. apply_acls.py sólo añade ACLs

Hay un punto crítico.

✅ Añade ACLs
❌ No borra

para el script de aplicación.
apply_acls.py continúa funcionando así:

--add solamente

entonces:

Git Rollback
≠
Kafka Rollback

porque las ACLs anteriormente aplicadas seguirán existiendo. Se necesitarás una capacidad adicional:

apply_acls.py --mode reconcile
o
apply_acls.py --mode delete

que compare:

Kafka actual
vs
YAML actual

y elimine ACLs sobrantes.

Recomendación para PROD

Documentar un procedimiento de tres niveles:

### Nivel 1 (antes de tocar Kafka)

git checkout -

o restaurar backup YAML.

### Nivel 2 (después de merge)

git revert <sha>

# Nivel 3 (después de aplicar ACLs)

1. Restaurar YAML backup
2. Reaplicar ACLs
3. Verificar kafka-acls --list

Mejora 

Implementar un script:

rollback_acl_request.py

Uso:

python3 rollback_acl_request.py --ticket RFC123456

Automáticamente:

1. Busca commit RFC123456
2. Ejecuta git revert
3. Regenera env/pro.yaml
4. Lanza apply_acls.py
5. Compara ACLs finales
6. Genera informe rollback.log





