// Databricks notebook source
/*******************************
Tämä notebook hakee automaattisesti 
uusimmat csv-tiedostot ja luo
niistä temp-taulut
*********************************/

/*anna tähän parametriksi source mitä halutaan käyttää*/
val source = "WINHIT"
/*sisältääkö tiedostot otsikkorivin?*/
val hdr_row = "true"


/*tietoaltaan app registration*/
val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "##",
  "dfs.adls.oauth2.credential" -> "##",
  "dfs.adls.oauth2.refresh.url" -> "##")


val src = List(source)
val hdr = List(hdr_row)

src.toDF.createOrReplaceTempView("source") //<-- tässä rekisteröidään source temp-tauluksi, niin se voidaan hakea muissakin soluissa. Tähän ei ilmeisesti atm ole muuta tapaa jakaa muuttujia eri solujen kesken.
hdr.toDF.createOrReplaceTempView("hdr")

/*unmount jos on jo mountattu*/
try {
        dbutils.fs.unmount("/mnt/" + source) //staging
        dbutils.fs.unmount("/mnt/storage/" + source) // storage
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
/*mountataan stage
Eli #ALLAS URL# voidaan viitata jatkossa --> /mnt/RIS
*/
dbutils.fs.mount(
  source = "ALLAS URL"+source+"/Kuopio",
  mountPoint = "/mnt/" + source,
  extraConfigs = configs)

/*Mountataan storage*/
dbutils.fs.mount(
  source = "ALLAS URL"+source+"/Kuopio",
  mountPoint = "/mnt/storage/" + source,
  extraConfigs = configs)

// COMMAND ----------

/*app registration*/
/*uusi allas, tämä tehdään jotta voidaan hakea csv-filuja data lake storesta*/
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "##")
spark.conf.set("dfs.adls.oauth2.credential", "##")
spark.conf.set("dfs.adls.oauth2.refresh.url", "TOKEN URL")

// COMMAND ----------

import java.io.File
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql

/*
Tämän koodin ideana on hakea äsken mountatusta adls-sijainnista automaattisesti kaikista kansioista uusimmat tiedostot ja tallentaa ne spark sql tauluiksi. Jonka jälkeen niille voidaan tehdä normaalit käsittelyt, esim. "sote-tietomalliin vienti".
*/

//haetaan source
val df = sqlContext.sql("SELECT value FROM source")
val src = df.select("value").first.getString(0)

//tämä funktio on kopsattu ja muokattu osoitteesta: https://rosettacode.org/wiki/Walk_a_directory/Recursively#Scala
  def walkTree(file: File): Iterable[File] = {
    val children = new Iterable[File] {
      def iterator = if (file.isDirectory) file.listFiles.iterator else Iterator.empty
    }
    //flatmap suorittaa walkTree funkkarin jokaiselle itemille children listassa, eli esim. jos children on /OBERON/ods_henkilostomaara/ niin funktio suoritetaan tuolle arvolle
    Seq(file) ++: children.flatMap(walkTree(_))
  }


val dir = new File("/dbfs/mnt/"+src+"/")
var files = new ListBuffer[String]()

  for(f <- walkTree(dir)
         if f.isDirectory() == false // haetaan vain tiedostot, kansiot ei kiinnosta
     )  files += f.toString()

val filesList = files.toList.toDF // <-- nyt tässä on listana/dataframena kaikki tiedostopolut

//tehdään temp taulu jota käytetään alla olevassa sql-kyselyssä
filesList.createOrReplaceTempView("filesList")


/*tässä haetaan viimeisin tiedosto. Ryhmitellään kansiorakenteen mukaan ja otetaan max tiedoston nimestä. Seuraavassa solussa on tämä kysely vähän helpompilukuisena.
Huom. Tämä on sinänsä tehty vähän karvalakkityyliin, eli oletetaan tietoaltaan päässä olevan rakenteen aina niin, että /-merkillä splitattuna 5:s solu on tiedoston nimi.
*/
val stagetaulut = sqlContext.sql("SELECT CONCAT(REPLACE(polku,'/dbfs',''),tiedosto) AS tiedosto,taulun_nimi FROM (SELECT LEFT(value,LENGTH(value)-LENGTH(split(value,'/')[5])) AS polku,MAX(split(value,'/')[5]) AS tiedosto,split(value,'/')[4] AS taulun_nimi FROM fileslist GROUP BY LEFT(value,LENGTH(value)-LENGTH(split(value,'/')[5])),split(value,'/')[4]) AS Results").toDF

stagetaulut.createOrReplaceTempView("stagetaulut")


// COMMAND ----------

/*Tällä saat helposti tarkastettua onko notebookin tunnuksella (tietoallas-ssis) oikeuksia kansioon, jos heittää herjaa niin ei ole oikeuksia, jos antaa listan kansioista, on oikeuksia*/
val kansiot = dbutils.fs.ls("/mnt/WINHIT/").toDF
kansiot.show

// COMMAND ----------


import org.apache.spark.sql.functions._
/*Luodaan temp-taulut*/
/*haetaan parametri hdr joka annettiin ekassa solussa, hdr määrittelee onko csv-filuissa ekalla rivillä sarakkeiden nimet vai ei*/
val df = sqlContext.sql("SELECT value FROM hdr")
val hdr = df.select("value").first.getString(0)

//haetaan tiedostojen polut ja luodaan uid joka riville
val stagetaulut = sqlContext.sql("SELECT tiedosto, taulun_nimi FROM stagetaulut").withColumn("UniqueID",monotonically_increasing_id)

//otetaan pelkät uid:t
val uid = stagetaulut.select("UniqueID").rdd.map(r => r(0)).collect().toList

//loopataan uid:t ja kerätään sen perusteella aina tiedoston polku ja nimi jolla se nimetään temp-tableksi
for(id <- uid) {
  val src = stagetaulut.select("tiedosto").filter("UniqueID="+id).first.getString(0)
  val nimi = stagetaulut.select("taulun_nimi").filter("UniqueID="+id).first.getString(0)
  val data = spark.read.format("csv").option("header", hdr).option("delimiter", "|").load(src) //tässä annetaan parametri
  data.createOrReplaceTempView(nimi)
  println(src + " AS " + nimi )
}

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP TABLE IF EXISTS D_hoitola_WINHIT;
// MAGIC CREATE TABLE D_hoitola_WINHIT
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM hoitola;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS F_kayntien_toimenpiteet_WINHIT;
// MAGIC CREATE TABLE F_kayntien_toimenpiteet_WINHIT
// MAGIC AS
// MAGIC SELECT `Työaika` AS Tyoaika, Hoitotyyppi, `Käyntitarkenne` AS Kayntitarkenne, AIkaleima, `Käyntihinnasto` AS Kayntihinnasto, Vuokra, `TyöajanTyyppi` AS TyoajanTyyppi, HoitolaOID,
// MAGIC HoitolaNro, HoitolaNimi, HoitolaKustannustili, HoitolaYtunnus, Huone, Alue, Palvelualue, Sopimus, Suorittajatyyppi, SuorittajaId, Suorittajatunnus, Suorittajanimi, Suorittajanimike,
// MAGIC Avustajatyyppi, AvustajaId, ValRYh, Recall, OpettajaTUnnus, OpettajaNro, Toimenpide_Avain, Toimenpide_Luokka, Toimenpide_Stakes, Toimenpide_Winhit, Hammasnumero, Pinnat, `Toimenpiteen nimi` AS Toimenpiteen_nimi,
// MAGIC `Lisäselite` AS Lisaselite, Valuutta, Hinta, Korvaus, Omavastuu, Toimenpide_Kesto, Toimenpide_hinnasto, TP_EHL, Passiivitoimenpide, ValryhTp, Muokattu, KukaMuokannut, Avustajatunnus, Avustajanimike, Kieli, AsiakasId,
// MAGIC AsiakasNimi, PostiNro, Kuntakoodi, Kuntanimi, `Kohderyhmä` AS Kohderyhma, Kohder_pvm, Kohder_A, Kohder_L, Varaustapa, Web, TutkimusTp, `TpideKesto yht` AS TpideKesto_yht, Huom, `Käyntityyppi` AS Kayntityyppi, `Toimenpide_lukumäärä` AS Toimenpide_lukumaara, Vuosi, Kuukausi, Viikko, `Syntymäaika` AS Syntymaaika, Palvelutapahtuma, Laskutus, Laskunumero, LaskutusPvm, `Syntymävuosi` AS Syntymavuosi, `Käynti_avain` AS Kaynti_avain,
// MAGIC `Käynti_Päivä` AS Kaynti_Paiva, `Käynti_Klo` AS Kaynti_Klo, `Käynti_Kesto` AS Kaynti_Kesto, `Ikä käynnillä` AS Ika_kaynnilla, `Ikä vuoden lopussa` AS Ika_vuoden_lopussa, Sukupuoli, `PassiivikoodiKäynnillä` AS PassiivikoodiKaynnilla
// MAGIC FROM kayntien_toimenpiteet;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS D_henkilo_WINHIT;
// MAGIC CREATE TABLE D_henkilo_WINHIT
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM henkilo;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS D_huoneet_WINHIT;
// MAGIC CREATE TABLE D_huoneet_WINHIT
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM huoneet;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS D_asiakas_WINHIT;
// MAGIC CREATE TABLE D_asiakas_WINHIT
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM asiakas;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS F_ajanvaraus_WINHIT;
// MAGIC CREATE TABLE F_ajanvaraus_WINHIT
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM ajanvaraus;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS F_recall_WINHIT;
// MAGIC CREATE TABLE F_recall_WINHIT
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM recall;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS F_hoitoonpaasy_WINHIT;
// MAGIC CREATE TABLE F_hoitoonpaasy_WINHIT
// MAGIC AS
// MAGIC SELECT *
// MAGIC FROM hoitoonpaasy;

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC -- käynnit erikseen kun on niin paljon sarakkeita (luettavuuden takia!)
// MAGIC DROP TABLE IF EXISTS F_kaynnit_WINHIT;
// MAGIC CREATE TABLE F_kaynnit_WINHIT
// MAGIC AS
// MAGIC SELECT `Käynti_avain` AS Kaynti_avain
// MAGIC       ,`Käynti_Päivä` AS Kaynti_Paiva
// MAGIC       ,`Käynti_Klo` AS Kaynti_Klo
// MAGIC       ,`Käynti_Kesto` AS Kaynti_Kesto
// MAGIC       ,`Työaika` AS Tyoaika
// MAGIC       ,`Hoitotyyppi`
// MAGIC       ,`Käyntitarkenne` AS Kayntitarkenne
// MAGIC       ,`Aikaleima`
// MAGIC       ,`Käyntihinnasto` AS Kayntihinnasto
// MAGIC       ,`Vuokra`
// MAGIC       ,`TyöajanTyyppi` AS TyoajanTyyppi
// MAGIC       ,`HoitolaOID`
// MAGIC       ,`HoitolaNro`
// MAGIC       ,`HoitolaNimi`
// MAGIC       ,`HoitolaKustannustili`
// MAGIC       ,`HoitolaYtunnus`
// MAGIC       ,`Huone`
// MAGIC       ,`Alue`
// MAGIC       ,`Palvelualue`
// MAGIC       ,`Sopimus`
// MAGIC       ,`Suorittajatyyppi`
// MAGIC       ,`SuorittajaId`
// MAGIC       ,`Suorittajatunnus`
// MAGIC       ,`Suorittajanimi`
// MAGIC       ,`Suorittajanimike`
// MAGIC       ,`Avustajatyyppi`
// MAGIC       ,`AvustajaId`
// MAGIC       ,`Avustajatunnus`
// MAGIC       ,`Avustajanimike`
// MAGIC       ,`Kieli`
// MAGIC       ,`AsiakasId`
// MAGIC       ,`AsiakasNimi`
// MAGIC       ,`PostiNro`
// MAGIC       ,`Kuntakoodi`
// MAGIC       ,`Kuntanimi`
// MAGIC       ,`Kohderyhmä` AS Kohderyhma
// MAGIC       ,`Kohder_pvm`
// MAGIC       ,`Kohder_A`
// MAGIC       ,`Kohder_L`
// MAGIC       ,`Varaustapa`
// MAGIC       ,`Web`
// MAGIC       ,`TutkimusTp`
// MAGIC       ,`TpideKesto yht` AS TpideKesto_yht
// MAGIC       ,`Huom`
// MAGIC       ,`Käyntityyppi`
// MAGIC       ,`Toimenpide_lukumäärä` AS Toimenpide_lukumaara
// MAGIC       ,`Vuosi`
// MAGIC       ,`Kuukausi`
// MAGIC       ,`Viikko`
// MAGIC       ,`Syntymäaika` AS Syntymaaika
// MAGIC       ,`Syntymävuosi` AS Syntymavuosi
// MAGIC       ,`Ikä käynnillä` AS Ika_kaynnilla
// MAGIC       ,`Ikä vuoden lopussa` AS Ika_vuoden_lopussa
// MAGIC       ,`Sukupuoli`
// MAGIC       ,`PassiivikoodiKäynnillä` AS PassiivikoodiKaynnilla
// MAGIC       ,`ValRyh`
// MAGIC       ,`Recall`
// MAGIC       ,`Valuutta`
// MAGIC       ,`Hinta`
// MAGIC       ,`Korvaus`
// MAGIC       ,`Omavastuu`
// MAGIC       ,`OpettajaTunnus`
// MAGIC       ,`OpettajaNro`
// MAGIC       ,`Palvelutapahtuma`
// MAGIC       ,`Topikoodi`
// MAGIC       ,`Ammatti`
// MAGIC       ,`Toteuttaja`
// MAGIC       ,`Palvelumuoto`
// MAGIC       ,`Yhteystapa`
// MAGIC       ,`Kävijäryhmä` AS Kavijaryhma
// MAGIC       ,`HoidonKiireellisyys`
// MAGIC       ,`KäynninLuonne` AS KaynninLuonne
// MAGIC       ,`Ensikäynti` AS Ensikaynti
// MAGIC       ,`KäyntiICD10` AS KayntiICD10
// MAGIC       ,`UlkoinenICD10`
// MAGIC       ,`TapaturmaICD10`
// MAGIC       ,`ICPC2`
// MAGIC       ,`Avohoito`
// MAGIC       ,`Jatkohoito`
// MAGIC       ,`SuunIndeksi_D`
// MAGIC       ,`SuunIndeksi_M`
// MAGIC       ,`SuunIndeksi_F`
// MAGIC       ,`SuunIndeksi_DMF`
// MAGIC       ,`SuunIndeksiMaito_d`
// MAGIC       ,`SuunIndeksiMaito_m`
// MAGIC       ,`SuunIndeksiMaito_f`
// MAGIC       ,`SuunIndeksiMaito_dmf`
// MAGIC       ,`Ienkudos`
// MAGIC       ,`TutkimustarpeenTiheys`
// MAGIC       ,`BOP`
// MAGIC       ,`PI`
// MAGIC       ,`Syvyystaso_mm`
// MAGIC       ,`Vetäytymätaso_mm` AS Vetaytymataso
// MAGIC       ,`Ientasku_> _4_mm` AS Ientasku_suurempi_kuin_4_mm
// MAGIC       ,`Ientasku_apexiin_saakka`
// MAGIC       ,`Laskutus`
// MAGIC FROM kaynnit;

// COMMAND ----------

/*Kirjoitetaan taulut data lake storeen*/

/*luettele tähän taulut mitkä halutaan kirjoittaa*/

val kirjoitettavat_taulut = 
List("D_hoitola_WINHIT",
"F_kayntien_toimenpiteet_WINHIT",
"D_henkilo_WINHIT",
"D_asiakas_WINHIT",
"F_ajanvaraus_WINHIT",
"F_recall_WINHIT",
"F_hoitoonpaasy_WINHIT",
"F_kaynnit_WINHIT"
    )

//haetaan source
val src_df = sqlContext.sql("SELECT value FROM source")
val src = src_df.select("value").first.getString(0)

val taulut = kirjoitettavat_taulut.toDF.withColumn("UniqueID",monotonically_increasing_id)


//otetaan pelkät uid:t
val uid = taulut.select("UniqueID").rdd.map(r => r(0)).collect().toList

//loopataan uid:t ja kirjoitetaan taulut
for(id <- uid) {
  val taulu = taulut.select("value").filter("UniqueID="+id).first.getString(0)
  
  val data = sqlContext.sql("select * FROM " + taulu)

data.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header","true").save("ALLAS URL"+src+"/Kuopio/" + taulu)
  println(taulu + " kirjoitettu kohteeseen " + "ALLAS URL"+src+"/Kuopio/" + taulu)
}

// COMMAND ----------


//tässä solussa poistetaan äskeisen kirjoituksen jäljiltä tiedostot joiden nimi on _started_, _SUCCESS_, _committed_
//tämä funktio on kopsattu ja muokattu osoitteesta: https://rosettacode.org/wiki/Walk_a_directory/Recursively#Scala
  def walkTree(file: File): Iterable[File] = {
    val children = new Iterable[File] {
      def iterator = if (file.isDirectory) file.listFiles.iterator else Iterator.empty
    }
    //flatmap suorittaa walkTree funkkarin jokaiselle itemille children listassa, eli esim. jos children on /OBERON/ods_henkilostomaara/ niin funktio suoritetaan tuolle arvolle
    Seq(file) ++: children.flatMap(walkTree(_))
  }

val dir = new File("/dbfs/mnt/storage/"+src+"/") //polku missä kansiot/tiedostot sijaitsevat
var files = new ListBuffer[String]() //listbuffer mihin kerätään tiedostot

  for(f <- walkTree(dir)
         if f.isDirectory() == false // haetaan vain tiedostot, kansiot ei kiinnosta
     )  files += f.toString()

val filesList = files.toList.toDF.select($"value",regexp_replace($"value","/dbfs","").as("polku")) // <-- nyt tässä on listana/dataframena kaikki tiedostopolut jotka halutaan poistaa.
// samalla luotiin "polku" sarake, jossa on tiedostojen polut ilman /dbfs -alkua, koska dbutils haluaa polun muodossa /mnt/jne
val poistettavat = filesList.filter($"value".contains("_committed_")).toDF.union(filesList.filter($"value".contains("_started_"))).toDF.union(filesList.filter($"value".contains("_SUCCESS")).toDF).select("polku").rdd.map(r => r(0)).collect().toList

for(p <- poistettavat) {
  dbutils.fs.rm(p.toString,false) 
}

// COMMAND ----------


