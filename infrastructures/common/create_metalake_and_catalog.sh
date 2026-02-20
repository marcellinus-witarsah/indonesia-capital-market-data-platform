#!/bin/bash

# =======================================================================
# Create metalake in Gravitino
# =======================================================================
metalake_name="capital_market_metalake"
response=$(curl http://localhost:8090/api/metalakes/$metalake_name)
if echo "$response" | grep -q "\"code\":0"; then
  true
else
  response=$(curl -X POST -H "Content-Type: application/json" -d '{"name":"'$metalake_name'","comment":"comment","properties":{}}' http://localhost:8090/api/metalakes)
  if echo "$response" | grep -q "\"code\":0"; then
    true # Placeholder, do nothing
  else
    echo "Metalake $metalake_name create failed"
    exit 1
  fi
fi

# =======================================================================
# Create data catalog in Gravitino
# =======================================================================
catalog_name="indonesia_capital_market_catalog"
response=$(curl http://localhost:8090/api/metalakes/$metalake_name/catalogs/$catalog_name)
if echo "$response" | grep -q "\"code\":0"; then
  true
else
  # Create Iceberg catalog for experience Gravitino service
  response=$(curl -X POST -H "Accept: application/vnd.gravitino.v1+json" -H "Content-Type: application/json" -d '{ "name":"'$catalog_name'", "type":"RELATIONAL", "provider":"lakehouse-iceberg", "comment":"comment", "properties":{ "uri":"jdbc:postgresql://postgres:5432/catalog_metastore_db", "catalog-backend":"jdbc", "warehouse":"s3a://iceberg/warehouse", "jdbc-user":"postgres", "jdbc-password":"postgres", "jdbc-driver":"org.postgresql.Driver"} }' http://localhost:8090/api/metalakes/$metalake_name/catalogs)
  if echo "$response" | grep -q "\"code\":0"; then
    true # Placeholder, do nothing
  else
    echo "create $catalog_name failed"
    exit 1
  fi
fi