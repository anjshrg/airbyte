package io.airbyte.integrations.destination.iceberg.config.catalog;

import static io.airbyte.integrations.destination.iceberg.IcebergConstants.CATALOG_NAME;

import java.util.HashMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

import org.jetbrains.annotations.NotNull;
import com.fasterxml.jackson.databind.JsonNode;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class GlueCatalogConfig extends IcebergCatalogConfig {

  private final String warehousePath;

  public GlueCatalogConfig(@NotNull JsonNode catalogConfigJson) {
    this.warehousePath = catalogConfigJson.get("warehouseUri").asText();
  }

  @Override
  public Map<String, String> sparkConfigMap() {
    Map<String, String> configMap = new HashMap<>();
    Map<String, String> properties = new HashMap<>(this.storageConfig.catalogInitializeProperties());
    configMap.put("spark.sql.catalog." + CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog");
    configMap.put("spark.sql.catalog." + CATALOG_NAME + ".warehouse", this.warehousePath);
    configMap.put("spark.sql.catalog." + CATALOG_NAME + ".catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
    configMap.put("spark.sql.catalog." + CATALOG_NAME + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    configMap.putAll(this.storageConfig.sparkConfigMap(CATALOG_NAME));
    properties.putAll(configMap);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.storageConfig.getWarehouseUri());
    return properties;
  }

  @Override
  public Catalog genCatalog() {
    GlueCatalog glueCatalog = new GlueCatalog();
    glueCatalog.initialize("airbyte-glue-catalog", sparkConfigMap());
    return glueCatalog;
  }
}
