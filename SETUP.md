# Setup

## Initialize Git repository

These labs are better tracked with Git.

Start by initializing a Git repository in this folder.

```sh
git init
```

Run the following command.

```sh
echo "ensta-bigdata-2024-$USER"
```

Create a GitLab/GitHub repository with that name, and set the `origin` remote to it.
If you don't know how to do that, please call for me.

Then push a first commit from the initial state of this directory.

## Install Java

Install the Java Development Kit (JDK) version 17.

Check that Java is installed.

```sh
java --version
```

Check that the `JAVA_HOME` environment variable is set in your shells.

```sh
echo $JAVA_HOME
```

Check that you can compile and run the "Hello World" program in `src/main/java/fr/ensta/bigdata/Main.java`.

## Install Maven

Check that Maven is installed.

```sh
mvn --version
```

## Get Hadoop

Download and extract Hadoop (version 3.4.1) in `./tmp/`.

```sh
mkdir -p tmp/
cd tmp/
curl https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.4.1.tar.gz -o hadoop-3.4.1.tar.gz
tar -xvzf hadoop-3.4.1.tar.gz
cd ../
```

Set `HADOOP_HOME` in your shell (you need to repeat it every time you open a shell).

```sh
export HADOOP_HOME="$(realpath ./tmp/hadoop-3.4.1)"
```

At this point, the following script should exit successfully.

```sh
./bin/check-system.sh
```

## Start HDFS

To start HDFS daemons on a single machine, you typically set up a pseudo-distributed Hadoop cluster, where all the components (NameNode, DataNode, and other services) run on a single machine. Here’s a step-by-step guide:

### Configure Hadoop

Update the configuration files in the `$HADOOP_HOME/etc/hadoop` directory:

`$HADOOP_HOME/etc/hadoop/core-site.xml`
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

### Format the HDFS Filesystem

Format the NameNode before starting HDFS for the first time:

```sh
"$HADOOP_HOME/bin/hdfs" namenode -format
```

### Start HDFS Daemons

Start the NameNode in a terminal.

```sh
"$HADOOP_HOME/bin/hdfs" namenode
```

Start the DataNode in another terminal (don't forget to set `$HADOOP_HOME`).

```sh
"$HADOOP_HOME/bin/hdfs" datanode
```

You may need to create your "home" directory in HDFS.

```sh
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "/user/$USER"
```

At this point, the following script should exit successfully.

```sh
./bin/check-hdfs.sh
```

Access the HDFS web interface by opening this URL in your browser:
<http://localhost:9870>
