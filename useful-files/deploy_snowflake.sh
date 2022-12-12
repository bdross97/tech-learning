# README:
#     - Please check the following URLs for the driver to pick up:
#         ODBC:  https://sfc-repo.snowflakecomputing.com/odbc/linux/index.html
#         JDBC:  https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/

export odbc_version=${odbc_version:-2.23.2}
export odbc_file=${odbc_file:-snowflake_linux_x8664_odbc-${odbc_version}.tgz}
cd /

echo "Downloading odbc driver version" ${odbc_version} "..."
curl -O https://sfc-repo.snowflakecomputing.com/odbc/linux/${odbc_version}/${odbc_file}

tar -xzvf ${odbc_file}
./snowflake_odbc/iodbc_setup.sh

SNOWSQL_DEST=/usr/bin 
