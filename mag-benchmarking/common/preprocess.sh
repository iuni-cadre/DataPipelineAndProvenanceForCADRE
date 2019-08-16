mkdir /raw/reduced-mags/mag-fos-data-processing

echo "Creating FieldsOfStudy.txt"
python filter_records.py /raw/mag/FieldsOfStudy.txt /raw/reduced-mags/mag-fos-data-processing/FieldsOfStudy.txt 2 -r "data processing"
#cp /raw/mag/FieldsOfStudy.txt /raw/reduced-mags/mag-fos-data-processing/FieldsOfStudy.txt

echo "Skipping FieldOfStudyChildren.txt since it is not necessary."
cp /raw/mag/FieldOfStudyChildren.txt /raw/reduced-mags/mag-fos-data-processing/FieldOfStudyChildren.txt

echo "Skipping RelatedFieldOfStudy.txt since it is not necessary."
cp /raw/mag/RelatedFieldOfStudy.txt /raw/reduced-mags/mag-fos-data-processing/RelatedFieldOfStudy.txt

echo "Creating PaperFieldsOfStudy.txt"
python filter_records.py /raw/mag/PaperFieldsOfStudy.txt /raw/reduced-mags/mag-fos-data-processing/PaperFieldsOfStudy.txt 1 -f int -t /raw/reduced-mags/mag-fos-data-processing/FieldsOfStudy.txt -k 0

echo "Creating Papers.txt"
python filter_records.py /raw/mag/Papers.txt /raw/reduced-mags/mag-fos-data-processing/Papers.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/PaperFieldsOfStudy.txt -k 0


echo "Creating PaperAuthorAffiliations.txt"
python filter_records.py /raw/mag/PaperAuthorAffiliations.txt /raw/reduced-mags/mag-fos-data-processing/PaperAuthorAffiliations.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 0

echo "Creating PaperLanguages.txt"
python filter_records.py /raw/mag/PaperLanguages.txt /raw/reduced-mags/mag-fos-data-processing/PaperLanguages.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 0

echo "Creating PaperResources.txt"
python filter_records.py /raw/mag/PaperResources.txt /raw/reduced-mags/mag-fos-data-processing/PaperResources.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 0

echo "Creating PaperReferences.txt"
python filter_records.py /raw/mag/PaperReferences.txt /raw/reduced-mags/mag-fos-data-processing/PaperReferences.txt 0 1 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 0

echo "Creating PaperRecommendations.txt"
python filter_records.py /raw/mag/PaperRecommendations.txt /raw/reduced-mags/mag-fos-data-processing/PaperRecommendations.txt 0 1 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 0

echo "Creating PaperCitationContexts.txt"
python clean_citation_contexts.py /raw/mag/PaperCitationContexts.txt /raw/mag-clean/PaperCitationContexts.txt
python filter_records.py /raw/mag-clean/PaperCitationContexts.txt /raw/reduced-mags/mag-fos-data-processing/PaperCitationContexts.txt 0 1 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 0

echo "Creating PaperAbstractsInvertedIndex.txt"
python filter_records.py /raw/mag/PaperAbstractsInvertedIndex.txt /raw/reduced-mags/mag-fos-data-processing/PaperAbstractsInvertedIndex.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 0

echo "Creating PaperUrls.txt"
python filter_records.py /raw/mag/PaperUrls.txt /raw/reduced-mags/mag-fos-data-processing/PaperUrls.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 0


echo "Creating Journals.txt"
python filter_records.py /raw/mag/Journals.txt /raw/reduced-mags/mag-fos-data-processing/Journals.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 10 -i

echo "Creating ConferenceSeries.txt"
python filter_records.py /raw/mag/ConferenceSeries.txt /raw/reduced-mags/mag-fos-data-processing/ConferenceSeries.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 11 -i

echo "Creating ConferenceInstances.txt"
python filter_records.py /raw/mag/ConferenceInstances.txt /raw/reduced-mags/mag-fos-data-processing/ConferenceInstances.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/Papers.txt -k 12 -i


echo "Creating Authors.txt"
python filter_records.py /raw/mag/Authors.txt /raw/reduced-mags/mag-fos-data-processing/Authors.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/PaperAuthorAffiliations.txt -k 1

echo "Creating Affiliations.txt"
python filter_records.py /raw/mag/Affiliations.txt /raw/reduced-mags/mag-fos-data-processing/Affiliations.txt 0 -f int -t /raw/reduced-mags/mag-fos-data-processing/PaperAuthorAffiliations.txt -k 2 -i
