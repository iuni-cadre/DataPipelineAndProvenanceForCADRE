export DATASET=mag-fos-data-processing
mkdir /raw/reduced-mags/$DATASET

echo "Creating FieldsOfStudy.txt"
python filter_records.py /raw/mag/FieldsOfStudy.txt /raw/reduced-mags/$DATASET/FieldsOfStudy.txt 2 -r "data processing"
#cp /raw/mag/FieldsOfStudy.txt /raw/reduced-mags/$DATASET/FieldsOfStudy.txt

echo "Skipping FieldOfStudyChildren.txt since it is not necessary."
cp /raw/mag/FieldOfStudyChildren.txt /raw/reduced-mags/$DATASET/FieldOfStudyChildren.txt

echo "Skipping RelatedFieldOfStudy.txt since it is not necessary."
cp /raw/mag/RelatedFieldOfStudy.txt /raw/reduced-mags/$DATASET/RelatedFieldOfStudy.txt

echo "Creating PaperFieldsOfStudy.txt"
python filter_records.py /raw/mag/PaperFieldsOfStudy.txt /raw/reduced-mags/$DATASET/PaperFieldsOfStudy.txt 1 -f int -t /raw/reduced-mags/$DATASET/FieldsOfStudy.txt -k 0

echo "Creating Papers.txt"
python filter_records.py /raw/mag/Papers.txt /raw/reduced-mags/$DATASET/Papers.txt 0 -f int -t /raw/reduced-mags/$DATASET/PaperFieldsOfStudy.txt -k 0


echo "Creating PaperAuthorAffiliations.txt"
python filter_records.py /raw/mag/PaperAuthorAffiliations.txt /raw/reduced-mags/$DATASET/PaperAuthorAffiliations.txt 0 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 0

echo "Creating PaperLanguages.txt"
python filter_records.py /raw/mag/PaperLanguages.txt /raw/reduced-mags/$DATASET/PaperLanguages.txt 0 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 0

echo "Creating PaperResources.txt"
python filter_records.py /raw/mag/PaperResources.txt /raw/reduced-mags/$DATASET/PaperResources.txt 0 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 0

echo "Creating PaperReferences.txt"
python filter_records.py /raw/mag/PaperReferences.txt /raw/reduced-mags/$DATASET/PaperReferences.txt 0 1 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 0

echo "Creating PaperRecommendations.txt"
python filter_records.py /raw/mag/PaperRecommendations.txt /raw/reduced-mags/$DATASET/PaperRecommendations.txt 0 1 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 0

echo "Creating PaperCitationContexts.txt"
python clean_citation_contexts.py /raw/mag/PaperCitationContexts.txt /raw/mag-clean/PaperCitationContexts.txt
python filter_records.py /raw/mag-clean/PaperCitationContexts.txt /raw/reduced-mags/$DATASET/PaperCitationContexts.txt 0 1 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 0

echo "Creating PaperAbstractsInvertedIndex.txt"
python filter_records.py /raw/mag/PaperAbstractsInvertedIndex.txt /raw/reduced-mags/$DATASET/PaperAbstractsInvertedIndex.txt 0 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 0

echo "Creating PaperUrls.txt"
python filter_records.py /raw/mag/PaperUrls.txt /raw/reduced-mags/$DATASET/PaperUrls.txt 0 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 0


echo "Creating Journals.txt"
python filter_records.py /raw/mag/Journals.txt /raw/reduced-mags/$DATASET/Journals.txt 0 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 10 -i

echo "Creating ConferenceSeries.txt"
python filter_records.py /raw/mag/ConferenceSeries.txt /raw/reduced-mags/$DATASET/ConferenceSeries.txt 0 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 11 -i

echo "Creating ConferenceInstances.txt"
python filter_records.py /raw/mag/ConferenceInstances.txt /raw/reduced-mags/$DATASET/ConferenceInstances.txt 0 -f int -t /raw/reduced-mags/$DATASET/Papers.txt -k 12 -i


echo "Creating Authors.txt"
python filter_records.py /raw/mag/Authors.txt /raw/reduced-mags/$DATASET/Authors.txt 0 -f int -t /raw/reduced-mags/$DATASET/PaperAuthorAffiliations.txt -k 1

echo "Creating Affiliations.txt"
python filter_records.py /raw/mag/Affiliations.txt /raw/reduced-mags/$DATASET/Affiliations.txt 0 -f int -t /raw/reduced-mags/$DATASET/PaperAuthorAffiliations.txt -k 2 -i
