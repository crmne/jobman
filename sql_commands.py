
def crazy_sql_command(viewname, cols, dicttab, keytab, id_col='id', dict_id='dict_id'):

    #create or replace view expview as select * from (select id as v1_id, value as nhid from
    #test0 where name='nhid') nhid LEFT OUTER JOIN (select id as v2_id, value as lrate from
    #test0 where name='lrate') lrate on nhid.v1_id = lrate.v2_id;

    col_queries = []
    colname0 = None
    for i, (colname, table_col) in enumerate(cols):
        safe_col = colname.replace('_','')   # get rid of underscores
        safe_col = safe_col.replace('.','_') # replace dots with underscores
      
        cols[i][0] = safe_col

        q = """LEFT OUTER JOIN 
               (select %(dict_id)s, %(table_col)s as %(safe_col)s from \"%(keytab)s\"
                where name='%(colname)s') %(safe_col)s 
                on %(safe_col)s.dict_id = %(dicttab)s.%(id_col)s""" % locals()
        
        col_queries.append(q)
       
    header = "create or replace view %s as select %s.%s, %s from %s " \
            % (viewname, dicttab, id_col, (", ".join([c[0] for c in cols])), dicttab)

    rval = header + "\n".join(col_queries)

    return rval
