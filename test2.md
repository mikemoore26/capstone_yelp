python -m pipeline \
--region us-west2  \
--input gs://yelp_bucket-mm/yelp_academic_dataset_checkin.json \
--output gs://yelp_bucket-mm/results/checkin \
--runner DataflowRunner \
--project algebraic-craft-367518 \
--temp_location gs://yelp_bucket-mm/tmp/


python -m pipeline  \
--region us-west2  \
--input gs://yelp_bucket-mm/yelp_academic_dataset_tip.json \
--output gs://yelp_bucket-mm/results/tip \
--runner DataflowRunner \
--project algebraic-craft-367518 \
--temp_location gs://yelp_bucket-mm/tmp/

python -m pipeline \
--region us-west2
--input gs://yelp_bucket-mm/yelp_academic_dataset_user.json \
--output gs://yelp_bucket-mm/results/user \
--runner DataflowRunner \
--project algebraic-craft-367518 \
--temp_location gs://yelp_bucket-mm/tmp/ 


python -m pipeline  \
--region us-west2 \
--input gs://yelp_bucket-mm/yelp_academic_dataset_review.json \
--output gs://yelp_bucket-mm/results/review \
--runner DataflowRunner \
--project algebraic-craft-367518 \
--temp_location gs://yelp_bucket-mm/tmp/


python -m pipeline  \
--region us-west2  \
--input gs://yelp_bucket-mm/yelp_academic_dataset_business.json \
--output gs://yelp_bucket-mm/results/business \
--runner DataflowRunner \
--project algebraic-craft-367518 \
--temp_location gs://yelp_bucket-mm/tmp/