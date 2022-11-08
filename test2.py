
for i in ['tip','checkin','business','review']:
    print(f'python -m ./test \
        --region us-west2  \
        --input gs://yelp_bucket-mm/yelp_academic_dataset_{i}.json \
        --output gs://yelp_bucket-mm/results/{i} \
        --runner DataflowRunner \
        --project algebraic-craft-367518 \
        --temp_location gs://yelp_bucket-mm/tmp/')