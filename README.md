# DbUtils

DbUtils is a utility tool to insert/update or query data from MongoDB. It can be used in automation scripts where you want to query data or you need some data to be stored or updated based on some conditions.

Basic command -
```sh
dbutils OPERATION [**OPERATION ARGS]
```

The following operation modes are available -
1. insert
2. query

## Insert mode
Insert mode is used for inserting new records or updating existing records based on conditions.

### Usage
```
$ dbutils insert -h

DbUtils - Insert mode

positional arguments:
  {insert}              Operation mode

optional arguments:
  -h, --help                show this help message and exit
  -database                 Mongodb Database Name
  -collection               Mongodb Collection Name
  -id-field                 ID field used for collection
  -auto-manage-timestamps   Specify true/false. If it is enabled, script will manage created_at, updated_at fields. (Default=true)
  -create-or-update         By providing this flag, if record with _id exists, it will be updated with new values.
  -input-file               Input file path
  -compare-fields           Comma-separated field names for comparision. (Default=[All fields])
  -compare-ignore-fields    Comma-separated field names to ignore for comparision. (Default=None)
```
#### Examples

1. Insert only
   If you just want to insert records without checking if similar record exists, you can run the insert mode without providing the `-create-or-update` flag.

   ```sh
   # Input file
   $ cat test._json

   {"name": "a", "value": "A"}
   {"name": "b", "value": "B"}


   # Insert content from the file
   $ dbutils insert -database Tmp -collection TmpCollection -input-file test._json

   [+] Completed, total inserted records = 2, total updated records = 0, total errors = 0.


   # Query insert records
   $ dbutils query -database Tmp -collection TmpCollection -output-mode stdout -output-file-type json -columns name,value,created_at

   {"name": "a", "value": "A", "created_at": {"$date": 1636280805294}}
   {"name": "b", "value": "B", "created_at": {"$date": 1636280805391}}
   ```
  2. Insert or Update records based on conditions
  Consider the following data is stored in TmpCollection -
        ```sh
        $ dbutils  query -database Tmp -collection TmpCollection -output-mode stdout -output-file-type json -columns name,value,created_at,updated_at

        {"name": "a", "value": "A", "created_at": {"$date": 1636281501457}}
        {"name": "b", "value": "Old value of B", "created_at": {"$date": 1636281501540}}
        ```
      And content of input file is -
        ```sh
        $ cat test._json

        {"name": "b", "value": "NEW VALUE"}
        {"name": "c", "value": "c"}
        ```

      Here the input file contains record for name==b and a document with the same name also exists in the database. By running the follwing command, document of name==b will be updated and document with name==c will b inserted.

      **Note:** If `-create-or-update` flag is provided, then `-id-field` is required, else it will not be possible to find old document for given record.

        ```sh
        # Insert or update records
        $ dbutils insert -database Tmp -collection TmpCollection -input-file test._json -create-or-update -id-field name -compare-fields value

        [+] Completed, total inserted records = 1, total updated records = 1, total errors = 0.

        # Query records, here the document 'b' is updated and a new field `updated_at` is also added
        dbutils  query -database Tmp -collection TmpCollection -output-mode stdout -output-file-type json -columns name,value,created_at,updated_at

        {"name": "a", "value": "A", "created_at": {"$date": 1636281501457}}
        {"name": "b", "value": "NEW VALUE", "created_at": {"$date": 1636281501540}, "updated_at": {"$date": 1636282327162}}
        {"name": "c", "value": "c", "created_at": {"$date": 1636282327164}}
        ```

## Query mode
Query mode is used for querying records from the database and storing output in json or csv format in different modes like file, file-chunks or directly showing the output to stdout.

### Usage
```
$ dbutils query -h

DbUtils - Insert mode
positional arguments:
  {query}                 Operation mode

optional arguments:
  -h, --help              show this help message and exit
  -database               Mongodb Database Name
  -collection             Mongodb Collection Name
  -columns                Given comma-separated values will be projected in the output (default=all columns)
  -batch-size             Batch size
  -limit                  Limit number of records.
  -output-mode            {file,file-chunks,stdout}
                          stdout output mode will print output in stdout
                          file output mode will write output to a file
                          file-chunks output mode will write output in smaller file chunks. Use batch-mode argument to specify batch size.
  -output-file-type       {csv,json}
                          Output file type
  -include-header         Include header for CSV file
  -output-path            If output-mode is file, provide file name. If output-mode is file-chunks, provide directory name
  -output-file-prefix     Output file prefix for file-chunks mode
  -output-file-extension  Output file extension for file-chunks mode
  -queries                Provide regex queries in this format - '-queries KEY_NAME_1=REGEX_1 KEY_NAME_2=REGEX-2'
```
#### Examples
1. Query data and print output on console
      ```sh
      # Following command will query documents if their name is either 'a' or 'c' (if it matches '^(a|c)$' regex)
      $ dbutils query -database Tmp -collection TmpCollection -output-mode stdout -output-file-type json -queries 'name=^(a|c)$'

      {"_id": {"$oid": "61875f4589cf35e65ec8948e"}, "name": "a", "value": "A", "created_at": {"$date": 1636281501457}}
      {"_id": {"$oid": "6187627fa03b2fd9500a34f4"}, "name": "c", "value": "c", "created_at": {"$date": 1636282327164}}
      ```
2. Query data and project selected fields using `-column` argument
      ```sh
      # JSON
      $ dbutils query -database Tmp -collection TmpCollection -output-mode stdout -output-file-type json -columns name,value

      {"name": "a", "value": "A"}
      {"name": "b", "value": "NEW VALUE"}
      {"name": "c", "value": "c"}


      # CSV
      $ dbutils query -database Tmp -collection TmpCollection -output-mode stdout -output-file-type csv -include-header true -columns name,value

      name,value
      a,A
      b,NEW VALUE
      c,c
      ```
3. Store output in file
      ```sh
      $ output_path="./output.csv"


      $ dbutils query -database Tmp -collection TmpCollection -output-mode "file" -output-path "$output_path" -output-file-type csv -columns name,value -include-header true


      # View output file
      $ cat "$output_path"

      name,value
      a,A
      b,NEW VALUE
      c,c
    ```
4. Store output in file-chunks
  For large number of records, if you want to store records in multiple smaller files, you can use `file-chunks` mode. Each file will contain at most 500 records by default, you can modify it by using `-batch-size` argument.
      ```sh
      # Data stored in db
      $ dbutils query -database Tmp -collection TmpCollection -output-mode stdout -output-file-type csv -columns name,value -include-header true

      name,value
      a,A
      b,NEW VALUE
      c,c


      # Write to file-chunks with batch-size=2
      $ dbutils query -database Tmp -collection TmpCollection -output-mode "file-chunks" -output-file-prefix "test-output" -output-file-extension "csv"  -batch-size 2 -output-path "./output-dir" -output-file-type csv -columns name,value -include-header true

      # View created files
      # Database contains three records, and for batch-size == 2, two files will be created. One file having first two records and the second file containing last record.
      $ ls "./output-dir"

      test-output-0.csv   test-output-1.csv


      $ cat "./output-dir/test-output-0.csv"

      name,value
      a,A
      b,NEW VALUE


      $ cat "./output-dir/test-output-1.csv"

      name,value
      c,c
      ```
