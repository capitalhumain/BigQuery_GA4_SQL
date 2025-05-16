DECLARE start_date DATE DEFAULT '2024-01-01'; -- Replace with your desired start date
DECLARE end_date DATE DEFAULT '2024-12-31'; -- Replace with your desired end date

-- Overview of Boolean metrics for search impressions
SELECT  
  SUM(CAST(is_amp_top_stories AS INT)) AS is_amp_top_stories, -- AMP Top Stories
  SUM(CAST(is_amp_blue_link AS INT)) AS is_amp_blue_link, -- AMP Blue Link
  SUM(CAST(is_job_listing AS INT)) AS is_job_listing, -- Job Listings
  SUM(CAST(is_job_details AS INT)) AS is_job_details, -- Job Details
  SUM(CAST(is_tpf_qa AS INT)) AS is_tpf_qa, -- Third-party Questions & Answers
  SUM(CAST(is_tpf_faq AS INT)) AS is_tpf_faq, -- Third-party FAQ
  SUM(CAST(is_tpf_howto AS INT)) AS is_tpf_howto, -- Third-party How-To
  SUM(CAST(is_weblite AS INT)) AS is_weblite, -- Web Lite
  SUM(CAST(is_action AS INT)) AS is_action, -- Action Items
  SUM(CAST(is_events_listing AS INT)) AS is_events_listing, -- Event Listings
  SUM(CAST(is_events_details AS INT)) AS is_events_details, -- Event Details
  SUM(CAST(is_search_appearance_android_app AS INT)) AS is_search_appearance_android_app, -- Android App Search Appearance
  SUM(CAST(is_amp_story AS INT)) AS is_amp_story, -- AMP Story
  SUM(CAST(is_amp_image_result AS INT)) AS is_amp_image_result, -- AMP Image Results
  SUM(CAST(is_video AS INT)) AS is_video, -- Video Results
  SUM(CAST(is_organic_shopping AS INT)) AS is_organic_shopping, -- Organic Shopping
  SUM(CAST(is_review_snippet AS INT)) AS is_review_snippet, -- Review Snippets
  SUM(CAST(is_special_announcement AS INT)) AS is_special_announcement, -- Special Announcements
  SUM(CAST(is_recipe_feature AS INT)) AS is_recipe_feature, -- Recipe Features
  SUM(CAST(is_recipe_rich_snippet AS INT)) AS is_recipe_rich_snippet, -- Recipe Rich Snippets
  SUM(CAST(is_subscribed_content AS INT)) AS is_subscribed_content, -- Subscribed Content
  SUM(CAST(is_page_experience AS INT)) AS is_page_experience, -- Page Experience
  SUM(CAST(is_practice_problems AS INT)) AS is_practice_problems, -- Practice Problems
  SUM(CAST(is_math_solvers AS INT)) AS is_math_solvers, -- Math Solvers
  SUM(CAST(is_translated_result AS INT)) AS is_translated_result, -- Translated Results
  SUM(CAST(is_edu_q_and_a AS INT)) AS is_edu_q_and_a, -- Educational Q&A
  SUM(CAST(is_product_snippets AS INT)) AS is_product_snippets, -- Product Snippets
  SUM(CAST(is_merchant_listings AS INT)) AS is_merchant_listings, -- Merchant Listings
  SUM(CAST(is_learning_videos AS INT)) AS is_learning_videos -- Learning Videos
FROM 
  `your-project-id.dataset_name.searchdata_url_impression` -- Replace with your actual project and dataset
WHERE 
  DATE(data_date) BETWEEN start_date AND end_date; -- Filter by declared date range
