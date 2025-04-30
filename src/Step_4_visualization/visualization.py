import os
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import seaborn as sns
from PIL import Image
import io
import base64
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
RESULTS_DIR = "path/to/results"  # Directory where analysis results are saved

# Page configuration
st.set_page_config(
    page_title="Steam Game Reviews Analysis",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add CSS for styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.8rem;
        color: #42A5F5;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    .card {
        background-color: #f5f5f5;
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1E88E5;
    }
    .metric-label {
        font-size: 1rem;
        color: #616161;
    }
</style>
""", unsafe_allow_html=True)

def load_data():
    """Load all analysis results"""
    data = {}
    
    # List of expected files
    expected_files = [
        'topic_keywords.csv',
        'review_aspects.csv',
        'aspect_frequencies.csv',
        'aspect_sentiment.csv',
        'aspect_sentiment_summary.csv',
        'helpfulness_model_report.csv',
        'helpfulness_feature_importances.csv',
        'game_insights.csv'
    ]
    
    # Try to load each file
    for file in expected_files:
        try:
            data[file.split('.')[0]] = pd.read_csv(os.path.join(RESULTS_DIR, file))
            logger.info(f"Loaded {file}")
        except Exception as e:
            logger.warning(f"Could not load {file}: {e}")
            data[file.split('.')[0]] = None
            
    return data

def create_wordcloud(keywords, title):
    """Create word cloud from keywords"""
    # Flatten keyword list if necessary
    if isinstance(keywords[0], list):
        all_keywords = [word for sublist in keywords for word in sublist]
    else:
        all_keywords = keywords
        
    # Convert to frequency dict
    word_freq = {word: all_keywords.count(word) for word in set(all_keywords)}
    
    # Create word cloud
    wc = WordCloud(width=800, height=400, background_color='white', max_words=100).generate_from_frequencies(word_freq)
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wc, interpolation='bilinear')
    ax.set_title(title)
    ax.axis('off')
    
    # Convert to image
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    
    return img_str

def overview_page(data):
    """Display overview of analysis results"""
    st.markdown("<h1 class='main-header'>Steam Game Reviews Analysis</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    This dashboard presents insights extracted from Steam game reviews to help indie game developers
    understand what aspects of their games players enjoy most. The analysis includes topic modeling,
    aspect extraction, sentiment analysis, and helpfulness prediction.
    """)
    
    # Display key metrics
    st.markdown("<h2 class='sub-header'>Key Metrics</h2>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Game count
    if data['game_insights'] is not None:
        game_count = len(data['game_insights']['game_id'].unique())
    else:
        game_count = "N/A"
        
    col1.markdown(f"""
    <div class='card'>
        <div class='metric-value'>{game_count}</div>
        <div class='metric-label'>Games Analyzed</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Review count
    review_count = "N/A"
    if data['game_insights'] is not None:
        review_count = data['game_insights']['total_reviews'].sum()
    
    col2.markdown(f"""
    <div class='card'>
        <div class='metric-value'>{review_count}</div>
        <div class='metric-label'>Total Reviews</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Average positive percentage
    avg_positive = "N/A"
    if data['game_insights'] is not None:
        avg_positive = f"{data['game_insights']['positive_percentage'].mean():.1f}%"
    
    col3.markdown(f"""
    <div class='card'>
        <div class='metric-value'>{avg_positive}</div>
        <div class='metric-label'>Avg. Positive %</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Most mentioned aspect
    most_mentioned = "N/A"
    if data['aspect_frequencies'] is not None:
        most_mentioned = data['aspect_frequencies'].iloc[0]['aspect']
    
    col4.markdown(f"""
    <div class='card'>
        <div class='metric-value'>{most_mentioned}</div>
        <div class='metric-label'>Top Aspect</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Display topic summary
    st.markdown("<h2 class='sub-header'>Review Topics Overview</h2>", unsafe_allow_html=True)
    
    if data['topic_keywords'] is not None:
        # Prepare keywords for word cloud
        try:
            keywords = data['topic_keywords']['keywords'].apply(eval).sum()
            img_str = create_wordcloud(keywords, "Common Topics in Reviews")
            st.markdown(f"<img src='data:image/png;base64,{img_str}' style='width:100%'>", unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Error creating word cloud: {e}")
            if isinstance(data['topic_keywords']['keywords'][0], str):
                st.dataframe(data['topic_keywords'])
            else:
                # Create a readable table of topics
                for i, row in data['topic_keywords'].iterrows():
                    st.markdown(f"**Topic {row['topic_id']}**: {', '.join(row['keywords'][:10])}")
    else:
        st.warning("Topic data not available. Please run the analysis pipeline first.")

def topic_analysis_page(data):
    """Display topic analysis results"""
    st.markdown("<h1 class='main-header'>Topic Analysis</h1>", unsafe_allow_html=True)
    
    if data['topic_keywords'] is None:
        st.warning("Topic data not available. Please run the analysis pipeline first.")
        return
        
    st.markdown("""
    This page shows detailed results of the topic modeling analysis. Topics are extracted using 
    Latent Dirichlet Allocation (LDA), which identifies groups of words that frequently appear 
    together in reviews.
    """)
    
    # Display topics
    st.markdown("<h2 class='sub-header'>Topic Keywords</h2>", unsafe_allow_html=True)
    
    # Create tabs for each topic
    topic_count = len(data['topic_keywords'])
    tabs = st.tabs([f"Topic {i}" for i in range(topic_count)])
    
    for i, tab in enumerate(tabs):
        with tab:
            topic_row = data['topic_keywords'][data['topic_keywords']['topic_id'] == i]
            if len(topic_row) > 0:
                keywords = topic_row['keywords'].values[0]
                
                # Convert string representation to list if needed
                if isinstance(keywords, str):
                    keywords = eval(keywords)
                
                # Display as word cloud
                try:
                    img_str = create_wordcloud(keywords, f"Topic {i} Keywords")
                    st.markdown(f"<img src='data:image/png;base64,{img_str}' style='width:100%'>", unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Error creating word cloud: {e}")
                
                # Display as list
                st.markdown("### Top Keywords")
                st.markdown(", ".join(keywords[:20]))
                
                # Show example reviews if available
                st.markdown("### Example Reviews")
                st.info("This feature requires additional integration with the review data.")

def aspect_analysis_page(data):
    """Display aspect analysis results"""
    st.markdown("<h1 class='main-header'>Aspect Analysis</h1>", unsafe_allow_html=True)
    
    if data['aspect_frequencies'] is None:
        st.warning("Aspect data not available. Please run the analysis pipeline first.")
        return
        
    st.markdown("""
    This page shows which aspects of games players mention most frequently in their reviews and
    the sentiment associated with each aspect.
    """)
    
    # Display aspect frequencies
    st.markdown("<h2 class='sub-header'>Aspect Frequencies</h2>", unsafe_allow_html=True)
    
    # Create bar chart
    fig = px.bar(
        data['aspect_frequencies'],
        x='count',
        y='aspect',
        orientation='h',
        title='Most Frequently Mentioned Game Aspects',
        labels={'count': 'Number of Mentions', 'aspect': 'Game Aspect'},
        color='count',
        color_continuous_scale=px.colors.sequential.Blues
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Display aspect sentiment
    st.markdown("<h2 class='sub-header'>Aspect Sentiment</h2>", unsafe_allow_html=True)
    
    if data['aspect_sentiment_summary'] is not None:
        # Create bar chart
        sentiment_df = data['aspect_sentiment_summary'].sort_values('mean', ascending=False)
        
        fig = px.bar(
            sentiment_df,
            x='aspect',
            y='mean',
            title='Average Sentiment by Game Aspect',
            labels={'mean': 'Sentiment Score (0-1)', 'aspect': 'Game Aspect'},
            color='mean',
            color_continuous_scale=px.colors.sequential.RdBu,
            text='count'
        )
        fig.update_layout(height=500)
        fig.add_shape(
            type="line",
            x0=-0.5,
            x1=len(sentiment_df) - 0.5,
            y0=0.5,
            y1=0.5,
            line=dict(color="red", width=2, dash="dash")
        )
        fig.update_traces(texttemplate='n=%{text}', textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("""
        The chart above shows the average sentiment score for each game aspect mentioned in reviews.
        A score above 0.5 (red dashed line) indicates positive sentiment, while a score below 0.5
        indicates negative sentiment. The number on each bar shows the count of reviews mentioning
        that aspect.
        """)
    else:
        st.warning("Aspect sentiment data not available.")

def game_insights_page(data):
    """Display game-specific insights"""
    st.markdown("<h1 class='main-header'>Game Insights</h1>", unsafe_allow_html=True)
    
    if data['game_insights'] is None:
        st.warning("Game insights data not available. Please run the analysis pipeline first.")
        return
        
    st.markdown("""
    This page provides insights for individual games based on their reviews.
    Select a game to see detailed analysis.
    """)
    
    # Game selector
    game_df = data['game_insights'].sort_values('total_reviews', ascending=False)
    selected_game = st.selectbox(
        "Select a game:",
        options=game_df['game_id'].tolist(),
        format_func=lambda x: f"{x} - {game_df[game_df['game_id'] == x]['game_name'].values[0][:50]}"
    )
    
    # Display game insights
    if selected_game:
        game_row = game_df[game_df['game_id'] == selected_game].iloc[0]
        
        st.markdown("<h2 class='sub-header'>Game Overview</h2>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        col1.markdown(f"""
        <div class='card'>
            <div class='metric-value'>{game_row['total_reviews']}</div>
            <div class='metric-label'>Total Reviews</div>
        </div>
        """, unsafe_allow_html=True)
        
        col2.markdown(f"""
        <div class='card'>
            <div class='metric-value'>{game_row['positive_percentage']:.1f}%</div>
            <div class='metric-label'>Positive Reviews</div>
        </div>
        """, unsafe_allow_html=True)
        
        col3.markdown(f"""
        <div class='card'>
            <div class='metric-value'>Topic {game_row['top_topic']}</div>
            <div class='metric-label'>Dominant Topic</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<h2 class='sub-header'>Review Content Analysis</h2>", unsafe_allow_html=True)
        
        # Display topic keywords
        st.markdown("### Key Topics")
        
        topic_keywords = game_row['topic_keywords']
        if isinstance(topic_keywords, str):
            topic_keywords = eval(topic_keywords)
            
        st.markdown(", ".join(topic_keywords))
        
        # Placeholder for more detailed analysis
        st.markdown("### What Players Like")
        st.info("This section would display the aspects that players mention positively for this specific game.")
        
        st.markdown("### Areas for Improvement")
        st.info("This section would display the aspects that players mention negatively for this specific game.")

def helpfulness_analysis_page(data):
    """Display helpfulness analysis results"""
    st.markdown("<h1 class='main-header'>Review Helpfulness Analysis</h1>", unsafe_allow_html=True)
    
    if data['helpfulness_feature_importances'] is None:
        st.warning("Helpfulness analysis data not available. Please run the analysis pipeline first.")
        return
        
    st.markdown("""
    This page shows the factors that make a review helpful to other players based on the
    votes received by reviews.
    """)
    
    # Display feature importances
    st.markdown("<h2 class='sub-header'>Factors That Make Reviews Helpful</h2>", unsafe_allow_html=True)
    
    # Create bar chart
    fig = px.bar(
        data['helpfulness_feature_importances'].sort_values('importance', ascending=False),
        x='importance',
        y='feature',
        orientation='h',
        title='Feature Importance for Review Helpfulness',
        labels={'importance': 'Importance Score', 'feature': 'Feature'},
        color='importance',
        color_continuous_scale=px.colors.sequential.Viridis
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Display model performance
    st.markdown("<h2 class='sub-header'>Model Performance</h2>", unsafe_allow_html=True)
    
    if data['helpfulness_model_report'] is not None:
        report_df = data['helpfulness_model_report']
        
        # Filter for overall metrics
        if 'accuracy' in report_df.columns:
            overall_metrics = report_df.loc[['accuracy', 'macro avg', 'weighted avg']]
            st.dataframe(overall_metrics)
        else:
            st.dataframe(report_df)
    else:
        st.warning("Model performance data not available.")
    
    # Guidelines for writing helpful reviews
    st.markdown("<h2 class='sub-header'>Guidelines for Writing Helpful Reviews</h2>", unsafe_allow_html=True)
    
    st.markdown("""
    Based on our analysis, here are some guidelines for writing reviews that other players find helpful:
    
    1. **Be specific** - Mention particular aspects of the game that you liked or disliked
    2. **Include context** - Mention your playtime and experience with similar games
    3. **Balance positives and negatives** - Even critical reviews should acknowledge positive aspects
    4. **Write with appropriate length** - Neither too short nor excessively long
    5. **Focus on the game itself** - Avoid unrelated complaints or discussions
    """)

def main():
    """Main dashboard function"""
    # Load data
    data = load_data()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["Overview", "Topic Analysis", "Aspect Analysis", "Game Insights", "Helpfulness Analysis"]
    )
    
    # Display selected page
    if page == "Overview":
        overview_page(data)
    elif page == "Topic Analysis":
        topic_analysis_page(data)
    elif page == "Aspect Analysis":
        aspect_analysis_page(data)
    elif page == "Game Insights":
        game_insights_page(data)
    elif page == "Helpfulness Analysis":
        helpfulness_analysis_page(data)
        
    # Sidebar additional info
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "This dashboard presents insights from Steam game reviews "
        "to help indie game developers understand player preferences."
    )
    
    # Add footer
    st.markdown("---")
    st.markdown("Developed for indie game developers to better understand player feedback.")

if __name__ == "__main__":
    main()