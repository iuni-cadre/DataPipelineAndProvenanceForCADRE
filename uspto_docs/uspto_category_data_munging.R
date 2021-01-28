
library(data.table)
  

pt <- as.data.frame(fread('/tmp/csv_for_janusgraph/nodes/patent2.tsv'))

pt[] <- lapply(pt, gsub, pattern='\n', replacement='')

pt[] <- lapply(pt, gsub, pattern='\\\\', replacement='')

pt[] <- lapply(pt, gsub, pattern='\"', replacement='')

pt[] <- lapply(pt, gsub, pattern='\t', replacement=' t')   

pt_sm <- subset(pt, pt$patent_id == '6888315')



#pt_part <- pt[1:100,]

fwrite(pt, 
       '//tmp/csv_for_janusgraph/nodes/patent3.tsv',
       quote=F, 
       col.names=T,
       sep='\t')


pt2 <- as.data.frame(fread('/tmp/csv_for_janusgraph/nodes/patent3.tsv'))

#########################


uspc_n <- as.data.frame(fread('/tmp/csv_for_janusgraph/categories/uspc_node2.tsv'))

uspc_n[] <- lapply(uspc_n, gsub, pattern='\n', replacement='')

uspc_n[] <- lapply(uspc_n, gsub, pattern='\\\\', replacement='')

uspc_n[] <- lapply(uspc_n, gsub, pattern='\"', replacement='')

uspc_n[] <- lapply(uspc_n, gsub, pattern='\t', replacement=' t')   

uspc_n_sm <- subset(uspc_n, uspc_n$patent_id == '6888315')



#uspc_n_part <- uspc_n[1:100,]

fwrite(uspc_n, 
       '/tmp/csv_for_janusgraph/categories/uspc_node3.tsv',
       quote=F, 
       col.names=T,
       sep='\t')


uspc_n2 <- as.data.frame(fread('/tmp/csv_for_janusgraph/categories/uspc_node3.tsv'))


#########################


uspc2patent <- as.data.frame(fread('/tmp/csv_for_janusgraph/categories/uspc_to_patent2.tsv'))

uspc2patent[] <- lapply(uspc2patent, gsub, pattern='\n', replacement='')

uspc2patent[] <- lapply(uspc2patent, gsub, pattern='\\\\', replacement='')

uspc2patent[] <- lapply(uspc2patent, gsub, pattern='\"', replacement='')

uspc2patent[] <- lapply(uspc2patent, gsub, pattern='\t', replacement=' t')   



fwrite(uspc2patent, 
       '/tmp/csv_for_janusgraph/categories/uspc_to_patent3.tsv',
       quote=F, 
       col.names=T,
       sep='\t')


uspc2patent2 <- as.data.frame(fread('/tmp/csv_for_janusgraph/categories/uspc_to_patent3.tsv'))


##################################################


acp <- as.data.frame(fread('/tmp/csv_for_janusgraph/edges/app_cites_patents_II.tsv'))


View(subset(acp, acp$citing_patent_id == 'RE28801'))
