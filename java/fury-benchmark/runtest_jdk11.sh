source ~/.bashrc
for v in {11,} ; do
  REGISTER_CLASS_FLAGS=('true')
  for flag in "${REGISTER_CLASS_FLAGS[@]}"; do
    echo "Oracle JDK$v Serialization: $flag"
    oracle_jdk $v
    REGISTER_CLASS=false java -jar target/benchmarks.jar "io.*\.UserTypeSerializeSuite\.*" -f 1 -wi 3 -i 3 -t 1 -w 2s -r 2s -rf csv
    cp jmh-result.csv jmh-result-jdk-$v-$flag-serialization.csv
    echo "Oracle JDK$v Deserialization: $flag"
    oracle_jdk $v
    REGISTER_CLASS=false java -jar target/benchmarks.jar "io.*\.UserTypeDeserializeSuite\.*" -f 1 -wi 3 -i 3 -t 1 -w 2s -r 2s -rf csv
    cp jmh-result.csv jmh-result-jdk-$v-$flag-deserialization.csv
  done
done
echo "Oracle JDK11 Serialization: ZeroCopySuite"
oracle_jdk 11
REGISTER_CLASS=false java -jar target/benchmarks.jar "io.*\.ZeroCopySuite\.*" -f 1 -wi 3 -i 3 -t 1 -w 2s -r 2s -rf csv
cp jmh-result.csv jmh-result-jdk-11-false-zerocopy-deserialization.csv
