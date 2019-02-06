package com.parallelmachines.reflex.common

import com.parallelmachines.reflex.components.ComponentAttribute
import com.parallelmachines.reflex.components.spark.batch.SparkBatchComponent

trait SnowFlakeCommon extends SparkBatchComponent{
  val sfURL = ComponentAttribute("sfURL", "", "URL",
    "URL for your Snowflake account.\n\nThe format of the URL domain is different depending on " +
      "the Snowflake Region where your account is located:\n\n" +
      "US West:\taccount_name.snowflakecomputing.com\n" +
      "Other regions:\taccount_name.region_id.snowflakecomputing.com\n\n\n" +
      "For example, if your account name is xy12345:\n\n" +
      "In US West, the URL would be xy12345.snowflakecomputing.com.\n" +
      "In US East, the URL would be xy12345.us-east-1.snowflakecomputing.com.\n" +
      "In EU (Frankfurt), the URL would be xy12345.eu-central-1.snowflakecomputing.com.\n" +
      "In East US 2, the URL would be xy12345.east-us-2.azure.snowflakecomputing.com.")
  val sfUser = ComponentAttribute("sfUser", "", "User", "Login name for the Snowflake user.")
  val sfPassword = ComponentAttribute("password", "", "Password",
    "Password for the Snowflake user.")
  val sfSchema = ComponentAttribute("sfSchema", "", "Schema",
    "The schema to use for the session after connecting.")
  val sfDatabase = ComponentAttribute("sfDatabase", "", "Database",
    "The database to use for the session after connecting.")
  val sfRole = ComponentAttribute("sfRole", "", "Role",
    "The default security role to use for the session after connecting.", optional = true)
  val sfWarehouse = ComponentAttribute("sfWarehouse", "", "Warehouse",
    "The default virtual warehouse to use for the session after connecting.", optional = true)

  attrPack.add(sfURL, sfUser, sfPassword, sfWarehouse, sfDatabase, sfSchema, sfRole)

  def getSnowFlakeParams(): Map[String, String] = {
    val url = sfURL.value
    val user = sfUser.value
    val password = sfPassword.value
    val schema = sfSchema.value
    val database = sfDatabase.value
    val role = sfRole.value
    val warehouse = sfWarehouse.value

    val sfBasic = Map(
      "sfSchema" -> schema,
      "sfPassword" -> password,
      "sfUser" -> user,
      "sfDatabase" -> database,
      "sfURL" -> url)

    var sfOptional = scala.collection.mutable.Map[String, String]()

    if (role != "") {
      sfOptional("sfRole") = role
    }

    if (warehouse != "") {
      sfOptional("sfWarehouse") = warehouse
    }

    return sfBasic ++ sfOptional
  }
}
