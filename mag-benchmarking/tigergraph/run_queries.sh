S=http://localhost:9000/query/mag
H="GSQL-TIMEOUT: 120000"

echo "Running find_paper" && time curl -H "$H" $S/find_paper?q=2625392185
echo "Running find_authors" && time curl -H "$H" $S/find_authors?q=2625392185
echo "Running find_fos" && time curl -H "$H" $S/find_fos?k=2764955546
echo "Running find_citations" && time curl -H "$H" $S/find_citations?q=2625392185
echo "Running find_citations2" && time curl -H "$H" $S/find_citations2?q=2625392185
echo "Running find_citations3" && time curl -H "$H" $S/find_citations3?q=2625392185
echo "Running find_citations4" && time curl -H "$H" $S/find_citations4?q=2625392185
echo "Running find_references" && time curl -H "$H" $S/find_references?q=2625392185
echo "Running find_references2" && time curl -H "$H" $S/find_references2?q=2625392185
echo "Running find_references3" && time curl -H "$H" $S/find_references3?q=2625392185
echo "Running find_references4" && time curl -H "$H" $S/find_references4?q=2625392185
echo "Running find_fos_paper_count" && time curl -H "$H" $S/find_fos_paper_count?g=138827492
echo "Running find_authors_by_paper_title" && time curl -H "$H" $S/find_authors_by_paper_title?s=%25big%20data%25
