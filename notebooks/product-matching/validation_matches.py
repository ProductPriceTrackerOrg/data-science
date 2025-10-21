import google.generativeai as genai
import pandas as pd
import os
import time
from tqdm import tqdm

# --- Configuration ---

def configure_gemini():
    """Configures the Gemini API key from environment variables."""
    try:
        GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
        genai.configure(api_key=GOOGLE_API_KEY)
        print("Gemini API key configured successfully.")
        return True
    except KeyError:
        print("Error: 'GOOGLE_API_KEY' environment variable not set.")
        print("Please set the environment variable before running the script.")
        return False

# 1. Initialize the model
# We use 'gemini-1.5-flash-latest' as the modern, fast "flash" model.
model = genai.GenerativeModel('gemini-1.5-flash-latest')

# 2. Define the prompt template for the "LLM Judge"
# This prompt is engineered for a precise 0 or 1 output.
PROMPT_TEMPLATE = """
You are an expert in e-commerce product matching. Your task is to determine if
two product titles refer to the exact same product.

You will be given two titles. Respond with ONLY the digit '1' if they are the
same product or '0' if they are different products. Do not add any explanation,
markdown, or other text.

---
Example 1:
Title 1: iphone 13
Title 2: samsung galaxy A55
Response: 0

Example 2:
Title 1: Galaxy A55 5G
Title 2: Samsung galaxy A55
Response: 1

Example 3:
Title 1: Apple iPhone 15 Pro (256GB) - Natural Titanium
Title 2: iPhone 15 Pro 256GB Natural Titanium
Response: 1
---

Task:
Title 1: {title1}
Title 2: {title2}
Response:
"""

# --- Main Functions ---

def load_data(filename="match_product_data.csv"):
    """Loads the product data from the specified CSV file."""
    try:
        df = pd.read_csv(filename)
        # Ensure the ground truth 'match' column is an integer
        df['match'] = df['match'].astype(int)
        print(f"Successfully loaded {len(df)} rows from {filename}.")
        return df
    except FileNotFoundError:
        print(f"Error: {filename} not found in the current directory.")
        return None
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None

def get_llm_judgment(title1, title2):
    """
    Sends the two titles to the Gemini model and returns a validated prediction.
    Returns:
        1: If model predicts a match
        0: If model predicts no match
       -1: If an error occurs (API error or invalid response)
    """
    prompt = PROMPT_TEMPLATE.format(title1=title1, title2=title2)
    try:
        # Generate the content
        response = model.generate_content(
            prompt,
            # Set safety settings to be less restrictive for this specific task
            safety_settings={
                'HARM_CATEGORY_HATE_SPEECH': 'BLOCK_NONE',
                'HARM_CATEGORY_HARASSMENT': 'BLOCK_NONE',
                'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'BLOCK_NONE',
                'HARM_CATEGORY_DANGEROUS_CONTENT': 'BLOCK_NONE'
            }
        )
        
        # Clean and validate the model's response
        prediction_text = response.text.strip()
        
        if prediction_text == "1":
            return 1
        elif prediction_text == "0":
            return 0
        else:
            # The model returned something other than "0" or "1"
            print(f"Warning: Invalid LLM response: '{prediction_text}'")
            return -1

    except Exception as e:
        # Handle API errors (e.g., rate limiting, content blocked)
        print(f"Warning: Error calling Gemini API: {e}")
        return -1

def main():
    """Main function to run the validation process."""
    if not configure_gemini():
        return

    input_file = "match_product_data.csv"
    output_file = "match_product_validation.csv"

    df = load_data(input_file)
    if df is None:
        return

    predictions = []
    
    print(f"Starting LLM validation for {len(df)} product pairs...")
    
    # Use tqdm for a nice progress bar
    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Validating"):
        title1 = row['title1']
        title2 = row['title2']
        
        prediction = get_llm_judgment(title1, title2)
        predictions.append(prediction)
        
        # IMPORTANT: Add a small delay to respect API rate limits.
        # 1.5-Flash allows 60 requests per minute by default.
        time.sleep(1.1) # Sleep for 1.1 seconds

    df['llm_prediction'] = predictions
    
    # --- Scoring ---
    
    # Filter out rows where the LLM failed (prediction == -1)
    valid_df = df[df['llm_prediction'] != -1].copy()
    
    total_rows = len(df)
    valid_count = len(valid_df)
    failed_count = total_rows - valid_count
    
    if valid_count == 0:
        print("\nError: No valid predictions were made by the LLM.")
        return

    # Calculate accuracy
    valid_df['is_correct'] = (valid_df['match'] == valid_df['llm_prediction'])
    correct_count = valid_df['is_correct'].sum()
    
    accuracy_score = (correct_count / valid_count) * 100
    
    # --- Report ---
    print("\n--- Validation Report ---")
    print(f"Total Rows Processed:         {total_rows}")
    print(f"Successful LLM Predictions: {valid_count}")
    print(f"Failed/Invalid LLM Responses: {failed_count}")
    print(f"Correct Predictions:          {correct_count}")
    print("---------------------------------")
    print(f"Final Accuracy Score: {accuracy_score:.2f} / 100")
    print("---------------------------------")
    
    # Save the results to a new file
    df.to_csv(output_file, index=False)
    print(f"Validation results with LLM predictions saved to {output_file}")

if __name__ == "__main__":
    main()