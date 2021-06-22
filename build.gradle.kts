plugins {
    kotlin("jvm") version "1.8.0"
    id("road-extraction")
}

group = "com.myproject"
version = "1.0-SNAPSHOT"

java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
    mavenCentral()
    jcenter()
}

val kotlinVer = "1.2.10"

dependencies {
    implementation("com.oracle:ojdbc6:11.2.0.3")
    implementation("junit:junit:4.12")
    compileOnly("org.apache.spark:spark-core_2.11:2.2.0.2.6.3.0-235")
    compileOnly("org.apache.spark:spark-streaming_2.11:2.2.0.2.6.3.0-235")
    compileOnly("org.apache.spark:spark-sql_2.11:2.2.0.2.6.3.0-235")
    compileOnly("org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.0.2.6.3.0-235")
    compileOnly("org.apache.spark:spark-hive_2.11:2.2.0")
    implementation("com.typesafe:config:1.3.3")
}
compileKotlin {
    kotlinOptions.jvmTarget = "1.8"
}
compileTestKotlin {
    kotlinOptions.jvmTarget = "1.8"
}