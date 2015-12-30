# Copyright 2015 Revolution Analytics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


is.server.running =
  function()
    length(grep(system("jps", intern = TRUE) , pattern = "SparkSubmit")) > 0

first.not.empty =
  function(...)
    detect(list(...), ~.!="")

dbConnect_retry =
  function(dr, url, retry, ...){
    if(retry > 0)
      tryCatch(
        dbConnect(drv = dr, url = url, ...),
        error =
          function(e) {
            Sys.sleep(0.1)
            dbConnect_retry(dr = dr, url = url, retry - 1, ...)})
    else dbConnect(drv = dr, url = url)}

src_HS2 =
  function(host, port, class, final.env, ...) {
    driverclass = "org.apache.hive.jdbc.HiveDriver"
    dr = JDBC(driverclass, Sys.getenv("HADOOP_JAR"))
    url = paste0("jdbc:hive2://", host, ":", port)
    con.class = paste0(class, "Connection")
    con =
      new(con.class, dbConnect_retry(dr, url, retry = 100, ...))
    pf = parent.frame()
    src_sql(
      c(class, "HS2"),
      con,
      info = list("Spark at", host = host, port = port),
      env = final.env,
      call = match.call(),
      calling.env = pf)}

src_Hive =
  function(
    host =
      first.not.empty(
        Sys.getenv("HIVE_SERVER2_THRIFT_BIND_HOST"),
        "localhost"),
    port =
      first.not.empty(
        Sys.getenv("HIVE_SERVER2_THRIFT_PORT"),
        10000),
        ...){
    src_HS2(host, port, "Hive", NULL, ...)}

src_desc.src_HS2 =
  function(x) {
    paste(x$info, collapse = ":")}

make.win.fun =
  function(f)
    function(...) {
      dplyr:::over(
        dplyr::build_sql(
          dplyr::sql(f),
          list(...)),
        dplyr:::partition_group(),
        NULL,
        frame = c(-Inf, Inf))}

src_translate_env.src_HS2 =
  function(x)
    sql_variant(
      scalar =
        sql_translator(
          .parent = base_scalar,
          rand =
            function(seed = NULL)
              build_sql(sql("rand"), if(!is.null(seed)) list(as.integer(seed))),
          randn =
            function(seed =  NULL)
              build_sql(sql("randn"), if(!is.null(seed)) list(as.integer(seed))),
          round =
            function(x, d = 0) build_sql(sql("round"), list(x, as.integer(d))),
          bround =
            function(x, d = 0) build_sql(sql("bround"), list(x, as.integer(d))),
          as.character = function (x) build_sql("CAST(", x, " AS STRING)"),
          as.numeric = function (x) build_sql("CAST(", x, " AS DOUBLE)")
          ),
      aggregate =
        sql_translator(
          .parent = base_agg,
          n = function() sql("COUNT(*)"),
          sd =  sql_prefix("STDDEV_SAMP"),
          var = sql_prefix("VAR_SAMP"),
          covar = sql_prefix("COVAR_POP"),
          cor = sql_prefix("CORR"),
          quantile = sql_prefix("PERCENTILE_APPROX")),
      window =
        sql_translator(
          .parent = base_win,
          n = function() sql("COUNT(*)"),
          sd =  make.win.fun("STDDEV_SAMP"),
          var = make.win.fun("VAR_SAMP"),
          covar = make.win.fun("COVAR_POP"),
          cor = make.win.fun("COR_POP"),
          quantile = make.win.fun("PERCENTILE_APPROX"),
          lag = function(x, n = 1L, default = NA, order = NULL) base_win$lag(x, as.integer(n), default, order),
          lead = function(x, n = 1L, default = NA, order = NULL) base_win$lead(x, as.integer(n), default, order),
          ntile = function (order_by, n) base_win$ntile(order_by, as.integer(n))
          ))

dedot = function(x) gsub("\\.", "_", x)

copy_to.src_Hive =
  function(dest, df, name =  dedot(deparse(substitute(df))), ...) {
    if(!name == dedot(name))
      warning("Replacing dot in table name with _ to appease spark")
    name = dedot(name)
    if(!all(names(df) == dedot(names(df))))
      warning("Replacing dot with _ in col names to appease spark")
    names(df) = dedot(names(df))
    dplyr:::copy_to.src_sql(dest, df, name, ...)}

load_to =
  function( dest, name, data, temporary, in.place, schema = NULL, ...)
    UseMethod("load_to")

load_to.src_Hive =
  function(
    dest,
    name,
    data,
    temporary = FALSE,
    in.place = TRUE,
    schema = NULL,
    ...) {
    types = {
      if(is.character(schema)) schema
      else {
        if (is.data.frame(schema)){
          setNames(db_data_type(dest$con, schema), colnames(schema))}
        else stop("Don't know how to extract a schema from this")}}
    if(!name == dedot(name))
      warning("Replacing dot in table name with _ to appease spark")
    name = dedot(name)
    db_create_table(
      con = dest$con,
      table = name,
      types = types,
      temporary = temporary,
      data = if(in.place) data)
    if(!in.place)
      db_load_table(con = dest$con, table = name, data)
    tbl(dest, name)}


#support inheritance
tbl.src_HS2 =
  function(src, from, ...){
    tbl_sql(
      map(strsplit(class(src)[1:2], "_"), 2),
      src = src,
      from = from, ...)}

tbls = function(src, ...) UseMethod("tbls")

tbls.src_sql =
  function(src, ...){
    frame = parent.frame()
    tblnames = db_list_tables(src$con)
    tblnames = keep(tblnames, ~db_has_table(my_db$con,.))
  invisible(map(tblnames, ~assign(., tbl(my_db, .), envir = frame)))}

tbls.default = function(src, ...) stop("Not implemented for non-sql srcs")

# refresh = function(x, ...) UseMethod("refresh")
#
# refresh.src_HS2 =
#   function(x, ...){
#     if(!identical(x$call$start.server, FALSE))
#       stop.server()
#     eval(x$call, envir = x$calling.env)}

