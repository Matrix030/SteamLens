# Review Theme Analysis - Original Code with Visualization
# ==========================================
# Cell 1: Install dependencies (run once)
!pip install sentence-transformers umap-learn hdbscan scikit-learn transformers matplotlib plotly wordcloud ipywidgets

# Cell 2: Imports - ORIGINAL CODE
import torch
import dask.dataframe as dd
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
import umap
import hdbscan
from sklearn.feature_extraction.text import TfidfVectorizer
from collections import defaultdict
from transformers import pipeline

# Additional imports for visualization
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import os
from IPython.display import HTML, display
import ipywidgets as widgets

# Cell 3: Load & sample reviews - Modified to handle failures gracefully
DF_PATH = '../Step_3_analysis/top_100_parquet/10.parquet'

# Function to create sample data if loading from files fails
def create_sample_data(n_positive=100, n_negative=50):
    """Create sample review data for demonstration purposes"""
    # Sample positive review templates
    positive_templates = [
        "I love this {item}! The {feature} is amazing and it's very {quality}.",
        "This {item} exceeded my expectations. Highly {quality} and worth every penny.",
        "Great {item}! The {feature} works perfectly and it's really {quality}.",
        "Absolutely love the {feature} on this {item}. It's so {quality} and intuitive.",
        "Best {item} I've ever purchased. The {feature} is {quality} and reliable."
    ]
    
    # Sample negative review templates
    negative_templates = [
        "Disappointed with this {item}. The {feature} is {issue} and not worth the money.",
        "This {item} broke after only {timeframe}. The {feature} is {issue}.",
        "Terrible {item}. The {feature} is completely {issue} and customer service was unhelpful.",
        "Avoid this {item}. The {feature} is {issue} and doesn't work as advertised.",
        "Waste of money. The {item}'s {feature} is {issue} and frustrating to use."
    ]
    
    # Sample values to fill in the templates
    items = ["product", "game", "device", "software", "tool", "appliance", "service"]
    features = ["interface", "design", "performance", "functionality", "quality", "reliability", "customer support"]
    qualities = ["excellent", "outstanding", "intuitive", "efficient", "reliable", "user-friendly", "innovative"]
    issues = ["terrible", "disappointing", "buggy", "unreliable", "confusing", "frustrating", "poorly designed"]
    timeframes = ["a week", "a few days", "one month", "the first use", "two weeks"]
    
    # Generate positive reviews
    positive_reviews = []
    for _ in range(n_positive):
        template = np.random.choice(positive_templates)
        item = np.random.choice(items)
        feature = np.random.choice(features)
        quality = np.random.choice(qualities)
        review = template.format(item=item, feature=feature, quality=quality)
        votes = np.random.randint(1, 50)
        positive_reviews.append({"review": review, "votes_up": votes, "voted_up": True, "review_language": "english"})
    
    # Generate negative reviews
    negative_reviews = []
    for _ in range(n_negative):
        template = np.random.choice(negative_templates)
        item = np.random.choice(items)
        feature = np.random.choice(features)
        issue = np.random.choice(issues)
        timeframe = np.random.choice(timeframes)
        review = template.format(item=item, feature=feature, issue=issue, timeframe=timeframe)
        votes = np.random.randint(1, 20)
        negative_reviews.append({"review": review, "votes_up": votes, "voted_up": False, "review_language": "english"})
    
    # Combine and return as a DataFrame
    all_reviews = positive_reviews + negative_reviews
    return pd.DataFrame(all_reviews)

# Try to read the parquet files, but fall back to sample data if it fails
try:
    print(f"Attempting to read data from {DF_PATH}...")
    df = dd.read_parquet(
        DF_PATH,
        columns=['review', 'votes_up', 'voted_up', 'review_language']
    )
    df = df[df['review_language'] == 'english'].persist()
    print(f"Successfully loaded data. Computing size...")
    size = df.shape[0].compute()
    print(f"Loaded {size} reviews.")
    using_sample_data = False
    
except Exception as e:
    print(f"Error loading parquet files: {e}")
    print("Creating sample data for demonstration instead...")
    df = create_sample_data()
    print(f"Created sample dataset with {len(df)} reviews.")
    using_sample_data = True

# Add a notice if using sample data
if using_sample_data:
    display(HTML("""
    <div style="background-color: #fff3cd; color: #856404; padding: 15px; border-radius: 5px; margin: 10px 0; border: 1px solid #ffeeba;">
        <strong>Notice:</strong> Using generated sample data for demonstration. The original data files could not be loaded.
        <p>To use your own data, please update the DF_PATH variable with the correct path to your parquet files.</p>
    </div>
    """))

# Function to sample docs & votes - Modified to handle both dask and pandas dataframes
def sample_bucket(df, label, n=50000, random_state=42):
    if isinstance(df, dd.DataFrame):
        # Original code for Dask DataFrame
        bucket = df[df['voted_up'] == label][['review', 'votes_up']].dropna()
        total = bucket.shape[0].compute()
        frac = min(1.0, n/total) if total > 0 else 0
        sampled = bucket.sample(frac=frac, random_state=random_state).compute() if frac > 0 else pd.DataFrame(columns=['review', 'votes_up'])
    else:
        # Modified code for Pandas DataFrame
        bucket = df[df['voted_up'] == label][['review', 'votes_up']].dropna()
        total = len(bucket)
        frac = min(1.0, n/total) if total > 0 else 0
        sampled = bucket.sample(frac=frac, random_state=random_state) if frac > 0 else pd.DataFrame(columns=['review', 'votes_up'])
    
    return sampled['review'].tolist(), sampled['votes_up'].tolist()

likes_docs, likes_votes = sample_bucket(df, True)
dis_docs, dis_votes = sample_bucket(df, False)

# Visualization 1: Display data statistics
def display_data_statistics():
    # This function uses variables from the original code (likes_docs, dis_docs)
    # Creating a visualization widget to show data statistics
    html_content = f"""
    <div style="background-color: #f0f8ff; padding: 20px; border-radius: 10px; margin: 10px;">
        <h2 style="text-align: center; color: #2c3e50;">Review Data Statistics</h2>
        <div style="display: flex; justify-content: space-around; flex-wrap: wrap;">
            <div style="background-color: #e8f4f8; padding: 15px; border-radius: 8px; margin: 10px; min-width: 200px; text-align: center;">
                <h3 style="color: #3498db;">Positive Reviews</h3>
                <p style="font-size: 24px; font-weight: bold;">{len(likes_docs)}</p>
            </div>
            <div style="background-color: #f8e8e8; padding: 15px; border-radius: 8px; margin: 10px; min-width: 200px; text-align: center;">
                <h3 style="color: #e74c3c;">Negative Reviews</h3>
                <p style="font-size: 24px; font-weight: bold;">{len(dis_docs)}</p>
            </div>
            <div style="background-color: #f0f0f0; padding: 15px; border-radius: 8px; margin: 10px; min-width: 200px; text-align: center;">
                <h3 style="color: #2c3e50;">Total Reviews</h3>
                <p style="font-size: 24px; font-weight: bold;">{len(likes_docs) + len(dis_docs)}</p>
            </div>
        </div>
        
        <div style="margin-top: 20px;">
            <h3 style="color: #2c3e50;">Sample Reviews</h3>
            <div style="display: flex; flex-wrap: wrap; justify-content: space-around;">
                <div style="background-color: #e8f4f8; padding: 15px; border-radius: 8px; margin: 10px; width: 45%;">
                    <h4 style="color: #3498db;">Positive Review Example</h4>
                    <p style="font-style: italic;">"{likes_docs[0] if likes_docs else 'No positive reviews'}"</p>
                </div>
                <div style="background-color: #f8e8e8; padding: 15px; border-radius: 8px; margin: 10px; width: 45%;">
                    <h4 style="color: #e74c3c;">Negative Review Example</h4>
                    <p style="font-style: italic;">"{dis_docs[0] if dis_docs else 'No negative reviews'}"</p>
                </div>
            </div>
        </div>
    </div>
    """
    display(HTML(html_content))

# Call the visualization function
display_data_statistics()

# Cell 4: Embed with SBERT (GPU fallback) - ORIGINAL CODE
device = 'cuda' if torch.cuda.is_available() else 'cpu'
try:
    embedder = SentenceTransformer('all-MiniLM-L6-v2', device=device)
    print(f"Loaded SBERT on {device}")
except Exception as e:
    print(f"SBERT GPU init failed ({e}), falling back to CPU")
    embedder = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')

# encode in batches to avoid OOM
def encode_docs(docs, batch_size=64):
    embeddings = []
    for i in range(0, len(docs), batch_size):
        chunk = docs[i:i+batch_size]
        emb = embedder.encode(chunk, convert_to_numpy=True)
        embeddings.append(emb)
    return np.vstack(embeddings) if embeddings else np.array([])

# Visualization 2: Progress Tracker
def display_progress(step, total_steps=4):
    steps = ['Loading Data', 'Creating Embeddings', 'Clustering Reviews', 'Extracting Insights']
    html_content = """
    <div style="background-color: #f0f8ff; padding: 20px; border-radius: 10px; margin: 10px;">
        <h2 style="text-align: center; color: #2c3e50;">Analysis Progress</h2>
        <div style="display: flex; justify-content: space-between; margin-top: 20px;">
    """
    
    for i in range(total_steps):
        if i < step:
            status = "Complete"
            color = "#2ecc71"  # Green for completed
            icon = "✓"
        elif i == step:
            status = "In Progress"
            color = "#3498db"  # Blue for in progress
            icon = "⟳"
        else:
            status = "Pending"
            color = "#95a5a6"  # Gray for pending
            icon = "○"
            
        html_content += f"""
        <div style="text-align: center; width: 23%;">
            <div style="background-color: {color}; color: white; padding: 10px; border-radius: 5px;">
                <span style="font-size: 18px;">{icon}</span>
                <h4 style="margin: 5px 0;">{steps[i]}</h4>
            </div>
            <p>{status}</p>
        </div>
        """
    
    html_content += """
        </div>
    </div>
    """
    
    display(HTML(html_content))

display_progress(1)  # Show we're at the embedding step

# Check if we have any documents to embed
if len(likes_docs) == 0 and len(dis_docs) == 0:
    print("No documents to analyze. Please check your data source.")
    likes_emb = np.array([])
    dis_emb = np.array([])
else:
    print(f"Embedding {len(likes_docs)} positive reviews...")
    likes_emb = encode_docs(likes_docs)
    print(f"Embedding {len(dis_docs)} negative reviews...")
    dis_emb = encode_docs(dis_docs)
    print("Embedding complete.")

# Display progress after embeddings
display_progress(2)

# Cell 5: UMAP + HDBSCAN clustering - MODIFIED to handle empty arrays
def cluster_labels(embeddings, min_cluster_size=None):
    if len(embeddings) == 0:
        return np.array([])
    
    # Set min_cluster_size based on data size
    if min_cluster_size is None:
        # Use 5% of the data size or a minimum of 2, whichever is larger
        min_cluster_size = max(2, int(len(embeddings) * 0.05))
    
    # Adjust n_neighbors based on data size
    n_neighbors = min(15, max(2, len(embeddings) - 1))
    
    # Adjust n_components based on data size
    n_components = min(10, max(2, len(embeddings) // 10))
    
    print(f"Clustering with min_cluster_size={min_cluster_size}, n_neighbors={n_neighbors}, n_components={n_components}")
    
    try:
        reducer = umap.UMAP(n_neighbors=n_neighbors, n_components=n_components, metric='cosine', random_state=42)
        umap_emb = reducer.fit_transform(embeddings)
        clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, metric='euclidean', cluster_selection_method='eom')
        return clusterer.fit_predict(umap_emb)
    except Exception as e:
        print(f"Clustering error: {e}")
        # Return all as noise (-1) if clustering fails
        return np.ones(len(embeddings), dtype=int) * -1

# Determine appropriate min_cluster_size based on data size
min_cluster_size_pos = max(2, len(likes_emb) // 20) if len(likes_emb) > 0 else 2
min_cluster_size_neg = max(2, len(dis_emb) // 20) if len(dis_emb) > 0 else 2

print(f"Clustering {len(likes_emb)} positive review embeddings...")
likes_labels = cluster_labels(likes_emb, min_cluster_size_pos)
print(f"Clustering {len(dis_emb)} negative review embeddings...")
dis_labels = cluster_labels(dis_emb, min_cluster_size_neg)

# Count clusters
if len(likes_labels) > 0:
    pos_clusters = len(set(likes_labels[likes_labels != -1]))
    print(f"Found {pos_clusters} positive review clusters")
else:
    pos_clusters = 0
    print("No positive review clusters found")

if len(dis_labels) > 0:
    neg_clusters = len(set(dis_labels[dis_labels != -1]))
    print(f"Found {neg_clusters} negative review clusters")
else:
    neg_clusters = 0
    print("No negative review clusters found")

# Update progress
display_progress(3)

# Cell 6: c-TF-IDF keyword extraction per cluster - ORIGINAL CODE
def extract_cluster_keywords(docs, labels, top_k=10):
    clusters = defaultdict(list)
    for doc, lab in zip(docs, labels):
        if lab == -1 or not doc.strip(): continue
        clusters[lab].append(doc)
    keywords = {}
    for lab, docs in clusters.items():
        text = " ".join(docs)
        try:
            tfidf = TfidfVectorizer(stop_words='english', max_features=50000, ngram_range=(1, 2))
            X = tfidf.fit_transform([text])
            terms = tfidf.get_feature_names_out()
            row = X.toarray().ravel()
            idxs = row.argsort()[-top_k:][::-1]
            keywords[lab] = [terms[i] for i in idxs]
        except ValueError as e:
            print(f"TF-IDF extraction error for cluster {lab}: {e}")
            keywords[lab] = []
    return keywords

print("Extracting keywords from positive review clusters...")
likes_keywords = extract_cluster_keywords(likes_docs, likes_labels)
print("Extracting keywords from negative review clusters...")
dis_keywords = extract_cluster_keywords(dis_docs, dis_labels)

# Cell 7: Assemble insights - ORIGINAL CODE
def assemble_insights(docs, labels, votes, keywords):
    counts, best = defaultdict(int), {}
    for doc, lab, v in zip(docs, labels, votes):
        if lab == -1: continue
        counts[lab] += 1
        if lab not in best or v > best[lab][0]: best[lab] = (v, doc)
    return {lab: {'keywords': keywords.get(lab, []), 'count': counts.get(lab, 0), 'example': best.get(lab, (None, ''))[1]} for lab in keywords}

print("Assembling insights from positive reviews...")
likes_insights = assemble_insights(likes_docs, likes_labels, likes_votes, likes_keywords)
print("Assembling insights from negative reviews...")
dis_insights = assemble_insights(dis_docs, dis_labels, dis_votes, dis_keywords)

# Original debug printing
print("\nLikes insights:", likes_insights)
print("\nDislikes insights:", dis_insights)

# Cell 8: Summarize each theme (GPU fallback) - MODIFIED to handle potential errors
try:
    print("Initializing summarizer...")
    summarizer = pipeline('summarization', model='sshleifer/distilbart-cnn-12-6', device=0 if torch.cuda.is_available() else -1)
    print("Loaded summarizer on", "GPU" if torch.cuda.is_available() else "CPU")
    
    def safe_summarize(text):
        if not text or len(text.strip()) < 30:
            return text
        try:
            return summarizer(text, max_length=40, min_length=15, do_sample=False, truncation=True)[0]['summary_text']
        except Exception as e:
            print(f"Summarization error: {e}")
            return text[:200] + '...' if len(text) > 200 else text
    
except Exception as e:
    print(f"Failed to initialize summarizer: {e}")
    print("Using simple truncation for summaries instead.")
    
    def safe_summarize(text):
        if not text:
            return ""
        words = text.split()
        if len(words) > 30:
            return ' '.join(words[:30]) + '...'
        return text

print("Generating summaries for positive review themes...")
for info in likes_insights.values():
    summary_text = info['example'] or ' '.join(info['keywords'])
    info['bullets'] = safe_summarize(summary_text)

print("Generating summaries for negative review themes...")
for info in dis_insights.values():
    summary_text = info['example'] or ' '.join(info['keywords'])
    info['bullets'] = safe_summarize(summary_text)

# Original debug printing
print("\nSummaries likes:", {lab: info['bullets'] for lab, info in likes_insights.items()})
print("\nSummaries dislikes:", {lab: info['bullets'] for lab, info in dis_insights.items()})

# Update progress
display_progress(4)

# Visualization 3: Display Final Results Dashboard
def create_insights_dashboard(likes_insights, dis_insights):
    # Create the overall dashboard container
    dashboard_html = """
    <div style="background-color: #f9f9f9; padding: 20px; border-radius: 10px; margin: 10px;">
                <h2 style="text-align: center; color: #2c3e50;">Theme Distribution</h2>
                <p style="text-align: center;">No themes were identified for visualization.</p>
            </div>
            """
            
    except Exception as e:
        print(f"Error creating charts: {e}")
        # Fallback to text-based display
        html_content = """
        <div style="background-color: #f9f9f9; padding: 20px; border-radius: 10px; margin: 10px;">
            <h2 style="text-align: center; color: #2c3e50;">Theme Distribution</h2>
            <div style="display: flex; justify-content: space-between; flex-wrap: wrap;">
        """
        
        # Check if we have positive insights
        if likes_insights:
            html_content += """
                <div style="width: 48%; min-width: 300px;">
                    <h3 style="color: #27ae60;">Positive Themes</h3>
                    <table style="width: 100%; border-collapse: collapse; margin-top: 10px;">
                        <tr style="background-color: #e8f8e8;">
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Theme</th>
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Count</th>
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Percentage</th>
                        </tr>
            """
            
            total_pos = sum(info['count'] for info in likes_insights.values()) or 1
            for lab, info in likes_insights.items():
                percentage = (info['count'] / total_pos) * 100
                html_content += f"""
                    <tr>
                        <td style="padding: 8px; text-align: left; border: 1px solid #ddd;">Theme #{lab+1}</td>
                        <td style="padding: 8px; text-align: left; border: 1px solid #ddd;">{info['count']}</td>
                        <td style="padding: 8px; text-align: left; border: 1px solid #ddd;">{percentage:.1f}%</td>
                    </tr>
                """
            
            html_content += """
                    </table>
                </div>
            """
        else:
            html_content += """
                <div style="width: 48%; min-width: 300px;">
                    <h3 style="color: #27ae60;">Positive Themes</h3>
                    <p>No positive themes identified.</p>
                </div>
            """
        
        # Check if we have negative insights
        if dis_insights:
            html_content += """
                <div style="width: 48%; min-width: 300px;">
                    <h3 style="color: #c0392b;">Negative Themes</h3>
                    <table style="width: 100%; border-collapse: collapse; margin-top: 10px;">
                        <tr style="background-color: #f8e8e8;">
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Theme</th>
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Count</th>
                            <th style="padding: 8px; text-align: left; border: 1px solid #ddd;">Percentage</th>
                        </tr>
            """
            
            total_neg = sum(info['count'] for info in dis_insights.values()) or 1
            for lab, info in dis_insights.items():
                percentage = (info['count'] / total_neg) * 100
                html_content += f"""
                    <tr>
                        <td style="padding: 8px; text-align: left; border: 1px solid #ddd;">Theme #{lab+1}</td>
                        <td style="padding: 8px; text-align: left; border: 1px solid #ddd;">{info['count']}</td>
                        <td style="padding: 8px; text-align: left; border: 1px solid #ddd;">{percentage:.1f}%</td>
                    </tr>
                """
            
            html_content += """
                    </table>
                </div>
            """
        else:
            html_content += """
                <div style="width: 48%; min-width: 300px;">
                    <h3 style="color: #c0392b;">Negative Themes</h3>
                    <p>No negative themes identified.</p>
                </div>
            """
        
        html_content += """
            </div>
        </div>
        """
        
        display(HTML(html_content))

# Display distribution charts
create_distribution_charts(likes_insights, dis_insights)

# Visualization 5: Create summary and recommendations
def create_summary_recommendations(likes_insights, dis_insights):
    # Check if we have any insights
    if not likes_insights and not dis_insights:
        display(HTML("""
        <div style="background-color: #f0f8ff; padding: 20px; border-radius: 10px; margin: 10px;">
            <h2 style="text-align: center; color: #2c3e50;">Executive Summary</h2>
            <p style="text-align: center;">No themes were identified for summarization.</p>
            <p style="text-align: center;">This could be due to insufficient data, or reviews that are too diverse to cluster effectively.</p>
        </div>
        """))
        return
    
    # Get top themes based on count
    top_pos_theme = sorted(likes_insights.items(), key=lambda x: x[1]['count'], reverse=True)[0] if likes_insights else None
    top_neg_theme = sorted(dis_insights.items(), key=lambda x: x[1]['count'], reverse=True)[0] if dis_insights else None
    
    html_content = """
    <div style="background-color: #f0f8ff; padding: 20px; border-radius: 10px; margin: 10px;">
        <h2 style="text-align: center; color: #2c3e50;">Executive Summary</h2>
        
        <div style="margin-top: 20px;">
            <h3>Key Findings</h3>
            <div style="display: flex; flex-wrap: wrap; justify-content: space-between;">
    """
    
    if top_pos_theme:
        lab, info = top_pos_theme
        html_content += f"""
        <div style="background-color: #e8f8e8; padding: 15px; border-radius: 8px; margin: 10px; width: 45%;">
            <h4 style="color: #27ae60;">Top Positive Theme: #{lab+1}</h4>
            <p><strong>Keywords:</strong> {', '.join(info['keywords'][:5])}</p>
            <p><strong>Summary:</strong> {info['bullets']}</p>
            <p><strong>Represents:</strong> {info['count']} reviews</p>
        </div>
        """
    
    if top_neg_theme:
        lab, info = top_neg_theme
        html_content += f"""
        <div style="background-color: #f8e8e8; padding: 15px; border-radius: 8px; margin: 10px; width: 45%;">
            <h4 style="color: #c0392b;">Top Negative Theme: #{lab+1}</h4>
            <p><strong>Keywords:</strong> {', '.join(info['keywords'][:5])}</p>
            <p><strong>Summary:</strong> {info['bullets']}</p>
            <p><strong>Represents:</strong> {info['count']} reviews</p>
        </div>
        """
    
    html_content += """
            </div>
            
            <h3 style="margin-top: 20px;">Recommendations</h3>
            <div style="background-color: white; padding: 15px; border-radius: 8px; margin-top: 10px;">
                <ol>
    """
    
    # Generate recommendations based on insights
    recommendations = []
    
    if top_pos_theme:
        recommendations.append(f"Leverage the strengths identified in the positive themes (especially '{', '.join(top_pos_theme[1]['keywords'][:3])}') in marketing materials and product messaging.")
        recommendations.append("Continue to build on what customers already appreciate about your product/service.")
    
    if top_neg_theme:
        recommendations.append(f"Address the most common pain points revealed in the negative themes (especially '{', '.join(top_neg_theme[1]['keywords'][:3])}') as development priorities.")
        recommendations.append("Develop a specific action plan to improve aspects customers are unsatisfied with.")
    
    recommendations.append("Continue monitoring customer sentiment to track improvement in problem areas over time.")
    recommendations.append("Consider implementing targeted customer outreach programs focused on addressing key concerns.")
    
    for rec in recommendations:
        html_content += f"<li style='margin-bottom: 8px;'>{rec}</li>"
    
    html_content += """
                </ol>
            </div>
            
            <h3 style="margin-top: 20px;">Next Steps</h3>
            <div style="background-color: white; padding: 15px; border-radius: 8px; margin-top: 10px;">
                <ul>
                    <li>Share these insights with product, marketing, and customer service teams</li>
                    <li>Develop action plans to address the top negative themes</li>
                    <li>Create messaging strategies that reinforce the positive themes</li>
                    <li>Set up tracking to measure improvement in problematic areas</li>
                    <li>Consider running a follow-up analysis in 3-6 months to track progress</li>
                </ul>
            </div>
        </div>
    </div>
    """
    
    display(HTML(html_content))

# Display summary and recommendations
create_summary_recommendations(likes_insights, dis_insights)

# Final dashboard display message
print("\n===== ANALYSIS COMPLETE =====")
print("The theme analysis dashboard has been created successfully.")
print("You can now view the visual representations of the review themes above.")

# Add export functionality (optional)
try:
    from IPython.display import FileLink
    
    # Save the analysis results to a JSON file
    import json
    
    analysis_results = {
        "positive_themes": {k: {
            "keywords": v["keywords"],
            "count": v["count"],
            "summary": v["bullets"],
            "example": v["example"]
        } for k, v in likes_insights.items()},
        "negative_themes": {k: {
            "keywords": v["keywords"],
            "count": v["count"],
            "summary": v["bullets"],
            "example": v["example"]
        } for k, v in dis_insights.items()}
    }
    
    with open('review_analysis_results.json', 'w') as f:
        json.dump(analysis_results, f, indent=2)
    
    print("\nAnalysis results have been saved to 'review_analysis_results.json'")
    display(FileLink('review_analysis_results.json', result_html_prefix="Click here to download the analysis results: "))
    
except Exception as e:
    print(f"Note: Could not create exportable results file: {e}")

        <h1 style="text-align: center; color: #2c3e50;">Review Analysis Dashboard</h1>
        <p style="text-align: center; color: #7f8c8d;">Automatically identifying themes in customer reviews</p>
        
        <div style="display: flex; justify-content: space-between; margin-top: 20px; flex-wrap: wrap;">
    """
    
    # Add positive insights section
    dashboard_html += """
            <div style="width: 48%; min-width: 300px; margin-bottom: 20px;">
                <h2 style="color: #27ae60;">Positive Review Themes</h2>
    """
    
    # Check if we have any positive insights
    if not likes_insights:
        dashboard_html += """
        <div style="background-color: #e8f8e8; padding: 15px; border-radius: 8px; margin-bottom: 15px;">
            <p>No positive review themes were identified. This could be due to insufficient data or low clustering quality.</p>
        </div>
        """
    else:
        # Add positive insights
        for lab, info in likes_insights.items():
            # Create a wordcloud-style display of keywords
            keywords_html = ""
            for kw in info['keywords']:
                # Random pastel green shade
                hue = np.random.randint(100, 140)  # Green hue range
                saturation = np.random.randint(60, 90)
                lightness = np.random.randint(75, 90)
                color = f"hsl({hue}, {saturation}%, {lightness}%)"
                
                # Font size based on keyword position (first keywords are more important)
                font_size = 16 - (info['keywords'].index(kw) * 0.5)
                
                keywords_html += f"""
                <span style="background-color: {color}; padding: 5px 10px; border-radius: 15px; 
                            margin: 5px; display: inline-block; font-size: {font_size}px;">
                    {kw}
                </span>
                """
                
            dashboard_html += f"""
            <div style="background-color: #e8f8e8; padding: 15px; border-radius: 8px; margin-bottom: 15px;">
                <h3 style="color: #27ae60;">Theme #{lab+1} ({info['count']} reviews)</h3>
                
                <div style="margin: 10px 0;">
                    <h4>Key Words & Phrases:</h4>
                    <div style="display: flex; flex-wrap: wrap;">
                        {keywords_html}
                    </div>
                </div>
                
                <div style="margin: 15px 0;">
                    <h4>Summary:</h4>
                    <p style="background-color: white; padding: 10px; border-radius: 5px;">{info['bullets']}</p>
                </div>
                
                <div style="margin: 15px 0;">
                    <h4>Representative Review:</h4>
                    <div style="background-color: white; padding: 10px; border-radius: 5px; border-left: 4px solid #27ae60;">
                        <p style="font-style: italic;">"{info['example']}"</p>
                    </div>
                </div>
            </div>
            """
    
    # Add negative insights section
    dashboard_html += """
            </div>
            <div style="width: 48%; min-width: 300px;">
                <h2 style="color: #c0392b;">Negative Review Themes</h2>
    """
    
    # Check if we have any negative insights
    if not dis_insights:
        dashboard_html += """
        <div style="background-color: #f8e8e8; padding: 15px; border-radius: 8px; margin-bottom: 15px;">
            <p>No negative review themes were identified. This could be due to insufficient data or low clustering quality.</p>
        </div>
        """
    else:
        # Add negative insights
        for lab, info in dis_insights.items():
            # Create a wordcloud-style display of keywords
            keywords_html = ""
            for kw in info['keywords']:
                # Random pastel red shade
                hue = np.random.randint(0, 20)  # Red hue range
                saturation = np.random.randint(60, 90)
                lightness = np.random.randint(75, 90)
                color = f"hsl({hue}, {saturation}%, {lightness}%)"
                
                # Font size based on keyword position (first keywords are more important)
                font_size = 16 - (info['keywords'].index(kw) * 0.5)
                
                keywords_html += f"""
                <span style="background-color: {color}; padding: 5px 10px; border-radius: 15px; 
                            margin: 5px; display: inline-block; font-size: {font_size}px;">
                    {kw}
                </span>
                """
                
            dashboard_html += f"""
            <div style="background-color: #f8e8e8; padding: 15px; border-radius: 8px; margin-bottom: 15px;">
                <h3 style="color: #c0392b;">Theme #{lab+1} ({info['count']} reviews)</h3>
                
                <div style="margin: 10px 0;">
                    <h4>Key Words & Phrases:</h4>
                    <div style="display: flex; flex-wrap: wrap;">
                        {keywords_html}
                    </div>
                </div>
                
                <div style="margin: 15px 0;">
                    <h4>Summary:</h4>
                    <p style="background-color: white; padding: 10px; border-radius: 5px;">{info['bullets']}</p>
                </div>
                
                <div style="margin: 15px 0;">
                    <h4>Representative Review:</h4>
                    <div style="background-color: white; padding: 10px; border-radius: 5px; border-left: 4px solid #c0392b;">
                        <p style="font-style: italic;">"{info['example']}"</p>
                    </div>
                </div>
            </div>
            """
    
    # Close the dashboard container
    dashboard_html += """
            </div>
        </div>
    </div>
    """
    
    display(HTML(dashboard_html))

# Display the insights dashboard
create_insights_dashboard(likes_insights, dis_insights)

# Visualization 4: Create theme distribution charts
def create_distribution_charts(likes_insights, dis_insights):
    try:
        # Check if we have any insights to visualize
        if not likes_insights and not dis_insights:
            display(HTML("""
            <div style="background-color: #f9f9f9; padding: 20px; border-radius: 10px; margin: 10px;">
                <h2 style="text-align: center; color: #2c3e50;">Theme Distribution</h2>
                <p style="text-align: center;">No themes were identified for visualization.</p>
            </div>
            """))
            return
        
        # Create theme count data
        pos_themes = [f"Positive Theme {lab+1}" for lab in likes_insights.keys()]
        pos_counts = [info['count'] for info in likes_insights.values()]
        
        neg_themes = [f"Negative Theme {lab+1}" for lab in dis_insights.keys()]
        neg_counts = [info['count'] for info in dis_insights.values()]
        
        # Determine if we should create plots
        create_pos_plot = len(pos_themes) > 0
        create_neg_plot = len(neg_themes) > 0
        
        if create_pos_plot or create_neg_plot:
            # Create figure with appropriate number of subplots
            if create_pos_plot and create_neg_plot:
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            else:
                fig, ax = plt.subplots(1, 1, figsize=(8, 6))
                if create_pos_plot:
                    ax1 = ax
                else:
                    ax2 = ax
            
            # Positive themes pie chart
            if create_pos_plot:
                ax1.pie(pos_counts, labels=pos_themes, autopct='%1.1f%%', 
                        colors=plt.cm.Greens(np.linspace(0.3, 0.7, len(pos_counts))))
                ax1.set_title('Distribution of Positive Themes')
            
            # Negative themes pie chart
            if create_neg_plot:
                ax2.pie(neg_counts, labels=neg_themes, autopct='%1.1f%%',
                       colors=plt.cm.Reds(np.linspace(0.3, 0.7, len(neg_counts))))
                ax2.set_title('Distribution of Negative Themes')
            
            plt.tight_layout()
            
            # Save figure for display
            plt.savefig('theme_distribution.png', dpi=300, bbox_inches='tight')
            
            # Display the charts
            display(HTML("""
            <div style="background-color: #f9f9f9; padding: 20px; border-radius: 10px; margin: 10px;">
                <h2 style="text-align: center; color: #2c3e50;">Theme Distribution</h2>
                <div style="text-align: center; margin-top: 15px;">
                    <img src="theme_distribution.png" style="max-width: 95%; border-radius: 5px;">
                </div>
            </div>
            """))
            
        else:
            # No themes to visualize
            display(HTML("""
            <div style="background-color: #f9f9f9; padding: 20px; border-radius: 10px; margin: 10px;">
    """))