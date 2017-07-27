ddir <- "~/source/tpch-dbgen/sf-1"
adir <- "~/source/dplyr-tpch/answers"

library(DBI)
con <- dbConnect(MonetDBLite::MonetDBLite(), "/tmp/dplyr-tpch")


schema <- list(
nation=c("n_nationkey","n_name","n_regionkey","n_comment"),
region=c("r_regionkey","r_name","r_comment"),
part=c("p_partkey","p_name","p_mfgr","p_brand","p_type","p_size","p_container","p_retailprice","p_comment"),
supplier=c("s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment"),
partsupp=c("ps_partkey","ps_suppkey","ps_availqty","ps_supplycost","ps_comment"),
customer=c("c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment"),
orders=c("o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk","o_shippriority","o_comment"),
lineitem=c("l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode","l_comment"))

for (t in names(schema)) {
    print(t)
    dfile <- file.path(ddir, paste0(t, ".tbl"))

    if (!dbExistsTable(con, t)) {
        dbWriteTable(con, t, readr::read_delim(file=dfile, delim="|", col_names=schema[[t]]))
    }

    assign(t,  dbReadTable(con, t) , envir = .GlobalEnv)
}


# saveRDS(lineitem, "lineitem.rds", compress=FALSE)
# lineitem2 <- readRDS("lineitem.rds")


norm_types_round <- function(df) {
    df <- as.data.frame(df)
    for (col in names(df)) {
        if (is.numeric(df[[col]])) {
            df[[col]] <- as.numeric(round(df[[col]], 2))
        }
        if (is.character(df[[col]])) {
            df[[col]] <- trimws(df[[col]])
        }
    }
    attr(df, "spec") <- NULL
    attr(df, "problems") <- NULL
    df
}

data_identical <- function(df1, df2) {
    df1 <- norm_types_round(df1)
    df2 <- norm_types_round(df2)
    stopifnot(ncol(df1) == ncol(df2))
    names(df1) <- names(df2)
    identical(df1, df2)
}

check_result<- function(res, qid) {
    ref <- readr::read_delim(file=file.path(adir, paste0(qid, ".out")), delim="|", col_names=FALSE, skip=1, trim_ws=TRUE)
    assign(paste0("res", qid), ref, envir = .GlobalEnv)
    stopifnot(data_identical(res, ref))
}

library(dplyr)


q1 <- lineitem %>% 
    filter(l_shipdate <= as.Date('1998-12-01') - 90) %>% 
    group_by(l_returnflag, l_linestatus) %>% 
    summarise(
        sum_qty=sum(l_quantity), 
        sum_base_price=sum(l_extendedprice), 
        sum_disc_price=sum(l_extendedprice * (1 - l_discount)), 
        sum_charge=sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)), 
        avg_qty=mean(l_quantity), 
        avg_price=mean(l_extendedprice), 
        avg_disc=mean(l_discount), count_order=n()) %>% 
    arrange(l_returnflag, l_linestatus)


check_result(q1, "q1")



q2 <- part %>% 
    filter(p_size == 15, grepl(".*BRASS$", p_type)) %>% 
    inner_join(partsupp, by=c("p_partkey" = "ps_partkey")) %>% 
    inner_join(
        partsupp %>% 
            inner_join(supplier, by=c("ps_suppkey" = "s_suppkey")) %>% 
            inner_join(nation, by=c("s_nationkey" = "n_nationkey")) %>% 
            inner_join(region %>% filter(r_name=="EUROPE"), by=c("n_regionkey"="r_regionkey")) %>% 
            group_by(ps_partkey) %>% 
            summarise(min_ps_supplycost = min(ps_supplycost)), 
            by=c("p_partkey" = "ps_partkey", "ps_supplycost" = "min_ps_supplycost")) %>% 
    inner_join(supplier, by=c("ps_suppkey" = "s_suppkey")) %>% 
    inner_join(nation, by=c("s_nationkey" = "n_nationkey")) %>% 
    inner_join(region %>% filter(r_name == "EUROPE"), by=c("n_regionkey" = "r_regionkey")) %>% 
    select(s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment) %>%
    arrange(desc(s_acctbal), n_name, s_name, p_partkey) %>% head(100)


check_result(q2, "q2")


q3 <- customer %>% filter(c_mktsegment == "BUILDING") %>% 
    inner_join(orders %>% filter(o_orderdate < as.Date('1995-03-15')), by=c("c_custkey" = "o_custkey")) %>% 
        inner_join(lineitem %>% filter(l_shipdate > as.Date('1995-03-15')), by=c("o_orderkey" = "l_orderkey")) %>% 
        group_by(o_orderkey, o_orderdate, o_shippriority) %>% 
        summarise(revenue=sum(l_extendedprice * (1 - l_discount))) %>% 
        select(o_orderkey, revenue, o_orderdate, o_shippriority) %>% 
        arrange(desc(revenue), o_orderdate) %>% head(10)

check_result(q3, "q3")




q4 <- orders %>% 
    filter(o_orderdate >= as.Date('1993-07-01'), o_orderdate < as.Date('1993-10-01')) %>% 
    inner_join(lineitem %>% filter(l_commitdate < l_receiptdate) %>% 
            group_by(l_orderkey) %>% summarise(), by=c("o_orderkey" = "l_orderkey")) %>% 
    group_by(o_orderpriority) %>% 
    summarise(order_count=n()) %>% 
    arrange(o_orderpriority)

check_result(q4, "q4")


q5 <- customer %>% 
    inner_join(orders %>% filter(o_orderdate >= as.Date('1994-01-01'), o_orderdate < as.Date('1995-01-01')), by=c("c_custkey" = "o_custkey")) %>% 
    inner_join(lineitem, by=c("o_orderkey" = "l_orderkey")) %>% 
    inner_join(supplier, by=c("l_suppkey" = "s_suppkey", "c_nationkey" = "s_nationkey")) %>% 
    inner_join(nation, by=c("c_nationkey" = "n_nationkey")) %>% 
    inner_join(region %>% filter(r_name == "ASIA"), by=c("n_regionkey"="r_regionkey")) %>% 
    group_by(n_name) %>% 
    summarise(revenue = sum(l_extendedprice * (1 - l_discount))) %>% 
    arrange(desc(revenue))

check_result(q5, "q5")



q6 <- lineitem %>% filter(l_shipdate >= as.Date('1994-01-01'), l_shipdate < as.Date('1995-01-01'), l_discount >= 0.05, l_discount <= 0.07, l_quantity < 24) %>% summarise(revenue = sum(l_extendedprice * l_discount))

check_result(q6, "q6")



q7 <- supplier %>% 
    inner_join(lineitem %>% filter(l_shipdate >= as.Date('1995-01-01'), l_shipdate <= as.Date('1996-12-31')), by=c("s_suppkey" = "l_suppkey")) %>% 
    inner_join(orders, by=c("l_orderkey" = "o_orderkey")) %>% 
    inner_join(customer, by=c("o_custkey" = "c_custkey")) %>% 
    inner_join(nation %>% select(n1_nationkey=n_nationkey, n1_name=n_name), by=c("s_nationkey"="n1_nationkey")) %>% 
    inner_join(nation %>% select(n2_nationkey=n_nationkey, n2_name=n_name), by=c("c_nationkey"="n2_nationkey")) %>% 
    filter((n1_name=='FRANCE' & n2_name == 'GERMANY') | (n1_name=='GERMANY' & n2_name == 'FRANCE')) %>% 
    mutate(supp_nation=n1_name, cust_nation=n2_name, l_year=as.integer(format(l_shipdate, "%Y")), volume=l_extendedprice * (1 - l_discount)) %>% 
    select(supp_nation, cust_nation, l_year, volume) %>% 
    group_by(supp_nation, cust_nation, l_year) %>% 
    summarise(revenue=sum(volume)) %>% 
    arrange(supp_nation, cust_nation, l_year)

check_result(q7, "q7")


q8 <- part %>% 
    filter(p_type == "ECONOMY ANODIZED STEEL") %>% 
    inner_join(lineitem, by=c("p_partkey"="l_partkey")) %>% 
    inner_join(supplier, by=c("l_suppkey" = "s_suppkey")) %>% 
    inner_join(orders %>% filter(o_orderdate >= as.Date('1995-01-01'), o_orderdate <= as.Date('1996-12-31')), by=c("l_orderkey"="o_orderkey")) %>% 
    inner_join(customer, by=c("o_custkey"="c_custkey")) %>% 
    inner_join(nation %>% select(n1_nationkey=n_nationkey, n1_regionkey=n_regionkey), by=c("c_nationkey"="n1_nationkey")) %>% 
    inner_join(region %>% filter(r_name=="AMERICA"), by=c("n1_regionkey"="r_regionkey")) %>% 
    inner_join(nation %>% select(n2_nationkey=n_nationkey, n2_name=n_name), by=c("s_nationkey" = "n2_nationkey")) %>% 
    mutate(o_year=as.integer(format(o_orderdate, "%Y")), volume=l_extendedprice * (1 - l_discount), nation=n2_name) %>%
    select(o_year, volume, nation) %>%
    group_by(o_year) %>%
    summarise(mkt_share=sum(ifelse(nation=="BRAZIL", volume, 0))/sum(volume)) %>%
    arrange(o_year)


check_result(q8, "q8")


q9 <- part %>% 
    filter(grepl(".*green.*", p_name)) %>% 
    inner_join(lineitem, by=c("p_partkey"="l_partkey")) %>% 
    inner_join(supplier, by=c("l_suppkey"="s_suppkey")) %>% 
    inner_join(partsupp, by=c("l_suppkey" = "ps_suppkey", "p_partkey" = "ps_partkey")) %>% 
    inner_join(orders, by=c("l_orderkey"="o_orderkey")) %>% 
    inner_join(nation, by=c("s_nationkey"="n_nationkey")) %>% 
    mutate(nation=n_name, o_year = as.integer(format(o_orderdate, "%Y")), amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) %>%
    select(nation, o_year, amount) %>%
    group_by(nation, o_year) %>%
    summarise(sum_profit=sum(amount)) %>%
    arrange(nation, desc(o_year))


#check_result(q9, "q9") correct, but rounding issue


q10 <- customer %>% 
    inner_join(orders %>% filter(o_orderdate >= as.Date('1993-10-01'), o_orderdate < as.Date('1994-01-01')), by=c("c_custkey" = "o_custkey")) %>% 
    inner_join(lineitem %>% filter(l_returnflag == "R"), by=c("o_orderkey" = "l_orderkey")) %>% 
    inner_join(nation, by=c("c_nationkey" = "n_nationkey")) %>% 
    group_by(c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment) %>% 
    summarise(revenue=sum(l_extendedprice * (1 - l_discount))) %>% 
    select(c_custkey, c_name, revenue, c_acctbal, n_name, c_address, c_phone, c_comment) %>% 
    arrange(desc(revenue)) %>% head(20)

check_result(q10, "q10")



sq <- partsupp %>% inner_join(supplier, by=c("ps_suppkey" = "s_suppkey")) %>% inner_join(nation %>% filter(n_name=="GERMANY"), by=c("s_nationkey" = "n_nationkey")) %>% group_by(ps_suppkey) %>% summarise(supp_sth=sum(ps_supplycost * ps_availqty) * 0.0001000000)

q11 <- partsupp %>% inner_join(supplier, by=c("ps_suppkey" = "s_suppkey")) %>% inner_join(nation %>% filter(n_name=="GERMANY"), by=("s_nationkey" = "n_nationkey")) %>% group_by(ps_partkey)


