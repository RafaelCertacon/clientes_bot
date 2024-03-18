from Function import criar_csv_do_banco_26, particionar_arquivo_principal, read_remove_escape_add_to_sql

criar_csv_do_banco_26()
particionar_arquivo_principal(parts=3, filename="Nota_Fiscal/tabela_csv.csv")
read_remove_escape_add_to_sql(num_partitions=3)
