start.server =
  function(
    opts = NULL,
    work.dir = getwd()){
    spark.home = Sys.getenv("SPARK_HOME")
    opts =
      paste0(
        paste0(
          ifelse(nchar(names(opts)) == 1, "-", "--"),
          names(opts),
          " ",
          map(opts,  ~if(is.null(.)) "" else .)),
        collapse = " ")
    server.cmd =
      paste0(
        "cd ", work.dir, ";",
        spark.home, "/sbin/start-thriftserver.sh", opts)
    retval = system(server.cmd, intern = TRUE)
    if(!is.null(attr(retval, "status")))
      stop("Couldn't start thrift server:", retval)}

stop.server =
  function(){
    spark.home = Sys.getenv("SPARK_HOME")
    system(
      paste0(
        spark.home,
        "/sbin/stop-thriftserver.sh"))}

src_SparkSQL =
  function(
    host =
      first.not.empty(
        Sys.getenv("HIVE_SERVER2_THRIFT_BIND_HOST"),
        "localhost"),
    port =
      first.not.empty(
        Sys.getenv("HIVE_SERVER2_THRIFT_PORT"),
        10000),
    start.server = FALSE,
    server.opts = list(),
    ...){
    final.env = NULL
    if(start.server) {
      do.call(
        "start.server",
        server.opts)
      final.env = new.env()
      reg.finalizer(
        final.env,
        function(e) {stop.server()},
        onexit = TRUE)}
    src_HS2(host, port, "SparkSQL", final.env, ...)}


# VALUES not supported, client-local file not supported
copy_to.src_SparkSQL =
  function(dest, df, name, ...)
    stop("copy not implemented for SparkSQL, use load_to instead")



ExternalData =
  function(parser, options)
    structure(
      list(parser = parser, options = discard(options, is.null)),
      class = "ExternalData")

CSVData =
  function(
    url,
    header = TRUE,
    delimiter = ",",
    quote = '"',
    parserLib = c("commons", "univocity"),
    mode = c("PERMISSIVE", "DROPMALFORMED", "FAILFAST"),
    charset = 'UTF-8',
    inferSchema = TRUE,
    comment = "#")
    ExternalData(
      "com.databricks.spark.csv",
      list(
        path = url,
        header = tolower(header),
        delimiter = delimiter,
        quote = quote,
        parserLib = match.arg(parserLib),
        mode = match.arg(mode),
        charset = charset,
        inferSchema = tolower(inferSchema),
        comment = comment))

JDBCData =
  function(
    url,
    dbtable,
    driver,
    partitionColumn = NULL,
    lowerBound = NULL,
    upperBound = NULL,
    numPartitions = NULL){
    opt.tally = is.null(partitionColumn) + is.null(lowerBound) +
      is.null(upperBound) + is.null(numPartitions)
    stopifnot(opt.tally == 0 || opt.tally == 4)
    ExternalData(
      "org.apache.spark.sql.jdbc",
      list(
        url = url,
        dbtable = dbtable,
        driver = driver,
        partitionColumn = partitionColumn,
        lowerBound = lowerBound,
        upperBound = upperBound,
        numPartitions = numPartitions))}

load_to.src_SparkSQL =
  function(dest, name, data, temporary = FALSE, in.place = FALSE, schema = NULL, ...) {
    types = {
      if(is.character(schema)) schema
      else {
        if (is.data.frame(schema)){
          setNames(db_data_type(dest$con, schema), colnames(schema))}}}
    stopifnot(!in.place)
    db_create_table(con = dest$con, table = name, types = types, temporary = temporary, external = in.place, using = data)
    tbl(dest, name)}


copy_to_from_local = #this to be used only when thrift server is local
  function(
    src,
    x,
    name,
    temporary = TRUE,
    location = {
      if(temporary)
        tempfile()
    else
      stop("Please provide location")}) {
    tmpdir = location
    dir.create(tmpdir)
    tmpfile = tempfile(tmpdir = tmpdir)
    write.table(x, file = tmpfile, sep = ",", col.names = TRUE, row.names = FALSE, quote = TRUE)
    load_to(src, data = CSVData(url = tmpdir), name = name, temporary = temporary, in.place = FALSE)} # , schema = x)} this doesn't work yet, reported
