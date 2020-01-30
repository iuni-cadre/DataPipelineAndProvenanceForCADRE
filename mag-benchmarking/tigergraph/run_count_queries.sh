S=http://localhost:9000/query/mag
H="GSQL-TIMEOUT: 120000"

echo "Running count_all_nodes" && time curl -H "$H" $S/count_all_nodes
echo "Running count_paper_nodes" && time curl -H "$H" $S/count_paper_nodes
echo "Running count_author_nodes" && time curl -H "$H" $S/count_author_nodes
echo "Running count_affiliation_nodes" && time curl -H "$H" $S/count_affiliation_nodes
echo "Running count_journal_nodes" && time curl -H "$H" $S/count_journal_nodes
echo "Running count_fos_nodes" && time curl -H "$H" $S/count_fos_nodes
echo "Running count_confinstance_nodes" && time curl -H "$H" $S/count_confinstance_nodes
echo "Running count_confseries_nodes" && time curl -H "$H" $S/count_confseries_nodes
echo "Running count_all_edges" && time curl -H "$H" $S/count_all_edges
echo "Running count_is_affiliated_with_edges" && time curl -H "$H" $S/count_is_affiliated_with_edges
echo "Running count_is_author_of_edges" && time curl -H "$H" $S/count_is_author_of_edges
echo "Running count_is_instance_of_edges" && time curl -H "$H" $S/count_is_instance_of_edges
echo "Running count_belongs_to_edges" && time curl -H "$H" $S/count_belongs_to_edges
echo "Running count_is_presented_at_edges" && time curl -H "$H" $S/count_is_presented_at_edges
echo "Running count_is_published_in_edges" && time curl -H "$H" $S/count_is_published_in_edges
echo "Running count_refers_to_edges" && time curl -H "$H" $S/count_refers_to_edges
echo "Running count_is_referred_by_edges" && time curl -H "$H" $S/count_is_referred_by_edges
