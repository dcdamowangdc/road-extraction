dependencies {
    implementation(project(":common"))
    implementation("com.oracle:ojdbc6:11.2.0.3")
    implementation("org.elasticsearch:elasticsearch-spark-20_2.11:6.3.0")
    implementation("junit:junit:4.12")
    compileOnly("org.apache.spark:spark-core_2.11:2.2.0.2.6.3.0-235")
    compileOnly("org.apache.spark:spark-streaming_2.11:2.2.0.2.6.3.0-235")
    compileOnly("org.apache.spark:spark-sql_2.11:2.2.0.2.6.3.0-235")
    compileOnly("org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.0.2.6.3.0-235")
    compileOnly("org.apache.spark:spark-hive_2.11:2.2.0")
    implementation("com.typesafe:config:1.3.3")

}
