// Databricks notebook source
/*mountataan OBERON-kansio databricksin tiedostojärjestelmään*/

try {
/*unmount jos on jo mountattu*/
dbutils.fs.unmount("/mnt/CLINISOFT")
dbutils.fs.unmount("/mnt/biopankki/CLINISOFT")
}
catch {
  case e: java.rmi.RemoteException =>
  {
    println("kansiota ei ole mountattu, joten unmounttaus ei onnistu!")
  }
  case e: Exception => {
    println("Unmounttaamisessa meni jokin pieleen.")
  }
}
//APP registration jotta voidaan lukea mountista
val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "##",
  "dfs.adls.oauth2.credential" -> "##",
  "dfs.adls.oauth2.refresh.url" -> "URL")

//CLinisoft tutkijan tietoallas
dbutils.fs.mount(
  source = "DATA URL",
  mountPoint = "/mnt/biopankki/CLINISOFT",
  extraConfigs = configs)

//Clinisoft päätietoallas
dbutils.fs.mount(
  source = "DATA URL",
  mountPoint = "/mnt/CLINISOFT",
  extraConfigs = configs)


// COMMAND ----------

/*uusi allas, tämä tehdään jotta voidaan hakea csv-filuja data lake storesta. Tähän annetaan app registrationin tiedot.*/
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "##")
spark.conf.set("dfs.adls.oauth2.credential", "##")
spark.conf.set("dfs.adls.oauth2.refresh.url", "URL")

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC -- ekaksi haetaan biopankki id
// MAGIC DROP TABLE IF EXISTS biopankki_id;
// MAGIC CREATE TEMPORARY TABLE biopankki_id
// MAGIC USING csv
// MAGIC OPTIONS (header "true",path "DATA URL");

// COMMAND ----------

import java.io.File
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql

/*
Haetaan clinisoft taulut
Tämän koodin ideana on hakea mountatusta adls-sijainnista automaattisesti kaikista kansioista uusimmat tiedostot ja tallentaa ne spark sql tauluiksi. Jonka jälkeen niille voidaan tehdä normaalit käsittelyt, esim. "sote-tietomalliin vienti".
*/

//tämä funktio on kopsattu ja muokattu osoitteesta: https://rosettacode.org/wiki/Walk_a_directory/Recursively#Scala
  def walkTree(file: File): Iterable[File] = {
    val children = new Iterable[File] {
      def iterator = if (file.isDirectory) file.listFiles.iterator else Iterator.empty
    }
    //flatmap suorittaa walkTree funkkarin jokaiselle itemille children listassa, eli esim. jos children on /OBERON/ods_henkilostomaara/ niin funktio suoritetaan tuolle arvolle
    Seq(file) ++: children.flatMap(walkTree(_))
  }

val dir = new File("/dbfs/mnt/CLINISOFT/")
var files = new ListBuffer[String]()

//loopataan directory läpi ja otetaan tiedostot, ei kansioita
  for(f <- walkTree(dir)
         if f.isDirectory() == true // haetaan vain tiedostot, kansiot ei kiinnosta
     )  files += f.toString()
val filesList = files.toList.toDF // <-- nyt tässä on listana/dataframena kaikki tiedostopolut

//tehdään temp taulu jota käytetään alla olevassa sql-kyselyssä
filesList.createOrReplaceTempView("filesList")


/*tässä haetaan viimeisin tiedosto. Ryhmitellään kansiorakenteen mukaan ja otetaan max tiedoston nimestä. Seuraavassa solussa on tämä kysely vähän helpompilukuisena.
Huom. Tämä on sinänsä tehty vähän karvalakkityyliin, eli oletetaan tietoaltaan päässä olevan rakenteen aina niin, että /-merkillä splitattuna 5:s solu on tiedoston nimi.
*/
val stagetaulut = sqlContext.sql("SELECT CONCAT(REPLACE(polku,'/dbfs',''),tiedosto) AS tiedosto,taulun_nimi FROM (SELECT LEFT(value,LENGTH(value)-LENGTH(split(value,'/')[4])) AS polku,MAX(split(value,'/')[4]) AS tiedosto,split(value,'/')[4] AS taulun_nimi FROM fileslist WHERE value LIKE '%CLINISOFT' GROUP BY LEFT(value,LENGTH(value)-LENGTH(split(value,'/')[4])),split(value,'/')[4]) AS Results").toDF
stagetaulut.createOrReplaceTempView("stagetaulut")

// COMMAND ----------


import org.apache.spark.sql.functions._
/*Luodaan temp-taulut edellisessä solussa tehdyllä stagetaulut-taulun datan perusteella*/
//haetaan tiedostojen polut ja luodaan uid joka riville
val stagetaulut = sqlContext.sql("SELECT tiedosto, taulun_nimi FROM stagetaulut WHERE tiedosto IS NOT NULL").withColumn("UniqueID",monotonically_increasing_id)

//otetaan pelkät uid:t
val uid = stagetaulut.select("UniqueID").rdd.map(r => r(0)).collect().toList

//loopataan uid:t ja kerätään sen perusteella aina tiedoston polku ja nimi jolla se nimetään temp-tableksi
for(id <- uid) {
  val src = stagetaulut.select("tiedosto").filter("UniqueID="+id).first.getString(0)
  val nimi = stagetaulut.select("taulun_nimi").filter("UniqueID="+id).first.getString(0)
  val data = spark.read.format("csv").option("header", "true").load(src) //tässä annetaan parametri
  data.createOrReplaceTempView(nimi)
  println(src + " AS " + nimi )
}

// COMMAND ----------

// MAGIC %sql
// MAGIC -- suodatetaan ekaksi patstats
// MAGIC 
// MAGIC DROP TABLE IF EXISTS F_PatStats_CLINISOFT_biopankki;
// MAGIC 
// MAGIC CREATE TABLE F_PatStats_CLINISOFT_biopankki
// MAGIC AS
// MAGIC SELECT 
// MAGIC       b.id AS biopankki_id
// MAGIC       ,SocSecurity AS Tietoallas_id
// MAGIC       ,PatientID
// MAGIC       ,AdminstWard
// MAGIC       ,AdminstWardName
// MAGIC       ,AdminstWardName2
// MAGIC       ,AdmissionA2
// MAGIC       ,AdmissionScore
// MAGIC       ,AdmissionTime
// MAGIC       ,AdmissionType
// MAGIC       ,AdmissionTypeName
// MAGIC       ,ArchStatus
// MAGIC       ,ArchTime
// MAGIC       ,BirthDate
// MAGIC       ,CareDays
// MAGIC       ,CostsUnit
// MAGIC       ,DischargeTime
// MAGIC       ,ForeNames
// MAGIC       ,HospComesFromCode
// MAGIC       ,HospComesFromName
// MAGIC       ,HospFuncStateCode
// MAGIC       ,HospFuncStateName
// MAGIC       ,HospGoesToCode
// MAGIC       ,HospGoesToName
// MAGIC       ,HospOutComeCode
// MAGIC       ,HospOutComeName
// MAGIC       ,ICUComesFromCode
// MAGIC       ,ICUComesFromName
// MAGIC       ,ICUDgCode
// MAGIC       ,ICUDgGroup
// MAGIC       ,ICUDgGroupName
// MAGIC       ,ICUDgName
// MAGIC       ,ICUGoesToCode
// MAGIC       ,ICUGoesToName
// MAGIC       ,ICUOutComeCode
// MAGIC       ,ICUOutComeName
// MAGIC       ,MaxExtTISS
// MAGIC       ,MaxExtTISSScore
// MAGIC       ,MaxScore
// MAGIC       ,MaxStdTISS
// MAGIC       ,MaxStdTISSScore
// MAGIC       ,MeanExtTISS
// MAGIC       ,MeanExtTISSScore
// MAGIC       ,MeanScore
// MAGIC       ,MeanStdTISS
// MAGIC       ,MeanStdTISSScore
// MAGIC       ,MRN
// MAGIC       ,Operated
// MAGIC       ,OperatedName
// MAGIC       ,OutCome6MonthCode
// MAGIC       ,OutCome6MonthName
// MAGIC       ,PatGroupCode
// MAGIC       ,PatGroupName
// MAGIC       ,PatNumber
// MAGIC       ,PrimOrganFCode
// MAGIC       ,PrimOrganFName
// MAGIC       ,SecOrganFCode
// MAGIC       ,SecOrganFName
// MAGIC       ,Sex
// MAGIC       ,SpecialtyCode
// MAGIC       ,SpecialtyName
// MAGIC       ,Surname
// MAGIC       ,TertOrganFCode
// MAGIC       ,TertOrganFName
// MAGIC       ,TotalExtTISS
// MAGIC       ,TotalStdTISS
// MAGIC FROM F_PatStats_CLINISOFT AS f
// MAGIC INNER JOIN biopankki_id AS b ON f.SocSecurity = b.ssn

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- otetaan clinisoft patientid:t jemmaan, sillä voidaan suodatella muita
// MAGIC DROP TABLE IF EXISTS clinisoft_idt;
// MAGIC CREATE TABLE clinisoft_idt
// MAGIC AS
// MAGIC SELECT DISTINCT patientid,tietoallas_id, biopankki_id
// MAGIC FROM F_PatStats_CLINISOFT_biopankki;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS F_ObservRec_CLINISOFT_biopankki;
// MAGIC CREATE TABLE F_ObservRec_CLINISOFT_biopankki
// MAGIC AS
// MAGIC SELECT c.biopankki_id, c.tietoallas_id,f.* FROM F_ObservRec_CLINISOFT AS f
// MAGIC INNER JOIN clinisoft_idt AS c ON f.PatientID = c.PatientID;
// MAGIC 
// MAGIC 
// MAGIC DROP TABLE IF EXISTS F_Patscores_CLINISOFT_biopankki;
// MAGIC CREATE TABLE F_Patscores_CLINISOFT_biopankki
// MAGIC AS
// MAGIC SELECT c.biopankki_id, c.tietoallas_id,f.*
// MAGIC FROM F_Patscores_CLINISOFT AS f
// MAGIC INNER JOIN clinisoft_idt AS c ON f.PatientID = c.PatientID;
// MAGIC 
// MAGIC --ward taulua ei tarvitse/pysty suodattelemaan, on ihan dimensiodataa
// MAGIC 
// MAGIC DROP TABLE IF EXISTS D_Wardref_CLINISOFT_biopankki;
// MAGIC CREATE TABLE D_Wardref_CLINISOFT_biopankki
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM D_Wardref_CLINISOFT

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- obervrec
// MAGIC SELECT c.biopankki_id, c.tietoallas_id,f.*
// MAGIC FROM F_Patscores_CLINISOFT AS f
// MAGIC INNER JOIN clinisoft_idt AS c ON f.PatientID = c.PatientID

// COMMAND ----------

//kirjoitellaan tietokantaan

/*tässä tehdään valmistelut yhteyttä varten*/
val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
val url = "ALLAS URL"
val db = "KOHDE DB"

//tähän pitäs joku secret scopes viritellä niin ei tarviais selkokielisenä passuja laitella
val username = "##"
val password = "##"

val jdbcUrl = s"jdbc:sqlserver://${url};database=${db}"

import java.util.Properties
val connectionProperties = new Properties()

connectionProperties.put("user", s"${username}")
connectionProperties.put("password", s"${password}")
connectionProperties.setProperty("Driver", driver)

import org.apache.spark.sql.SaveMode
/*tässä tapahtuu varsinainen kirjoitus*/

//tässä siis kirjoitetaan suoraan Biopankille omistettuun SQL-tietokantaan, eikä datalakeen takaisin. Tässä casessa ei nyt käytetä datoja datalakessa mutta saa ne sinnekin sit helposti.
/*clinisoft*/
//spark.sql("SELECT * FROM F_Patscores_CLINISOFT_biopankki").write.mode(SaveMode.Overwrite).jdbc(jdbcUrl,"F_Patscores_CLINISOFT",connectionProperties)
//spark.sql("SELECT * FROM D_Wardref_CLINISOFT_biopankki").write.mode(SaveMode.Overwrite).jdbc(jdbcUrl,"D_Wardref_CLINISOFT",connectionProperties)
//spark.sql("SELECT * FROM F_ObservRec_CLINISOFT_biopankki").write.mode(SaveMode.Overwrite).jdbc(jdbcUrl,"F_ObservRec_CLINISOFT",connectionProperties) // tämä on ihan älyttömän hidas!
spark.sql("SELECT * FROM F_PatStats_CLINISOFT_biopankki").write.mode(SaveMode.Overwrite).jdbc(jdbcUrl,"F_PatStats_CLINISOFT",connectionProperties)

// COMMAND ----------


