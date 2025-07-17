```
 ╔═══════════════════════════════════════════════════════════════════════════════════════════╗
 ║ ███████╗████████╗███████╗ █████╗  ███╗ ███╗ ██╗     ███████╗███╗   ██╗███████╗ █████╗ ██╗ ║
 ║ ██╔════╝╚══██╔══╝██╔════╝██╔══██╗████╗ ████║██║     ██╔════╝████╗  ██║██╔════╝██╔══██╗██║ ║
 ║ ███████╗   ██║   █████╗  ███████║██╔████╔██║██║     █████╗  ██╔██╗ ██║███████╗███████║██║ ║
 ║ ╚════██║   ██║   ██╔══╝  ██╔══██║██║╚██╔╝██║██║     ██╔══╝  ██║╚██╗██║╚════██║██╔══██║██║ ║
 ║ ███████║   ██║   ███████╗██║  ██║██║ ╚═╝ ██║███████╗███████╗██║ ╚████║███████║██║  ██║██║ ║
 ║ ╚══════╝   ╚═╝   ╚══════╝╚═╝ ╚═╝ ╚═╝    ╚═╝╚══════╝ ╚══════╝╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝╚═╝ ║
 ╚═══════════════════════════════════════════════════════════════════════════════════════════╝
```

# 🎮 steamLensAI: Game Developer Analytics Platform
**Leveraged my RTX 4080 Super as a mini-cluster by using Dask's LocalCluster to parallelize multiple transformer instances on a single GPU for high-throughput inference**

steamLensAI is a specialized analytics platform that helps game developers understand what players really think about their games. Instead of manually reading thousands of reviews, get AI-powered insights organized by game themes—from gameplay mechanics to graphics, story, and performance. Know exactly what to fix, what to keep, and what players love most.

![Example Output](output_example.png)

## 🎯 Why steamLensAI?

### For Game Developers
- **Prioritize Development**: See which game aspects need immediate attention
- **Understand Player Pain Points**: Get clear summaries of what frustrates players
- **Identify Strengths**: Know what players love so you can build on it
- **Data-Driven Decisions**: Make informed choices about updates and patches
- **Competitive Analysis**: Understand player sentiment across different games

### Built for Scale
- Process **millions of reviews** in minutes, not days
- **GPU-accelerated** analysis for enterprise-scale datasets
- **Real-time monitoring** of processing pipelines
- **Automated insights** without manual review reading

## ✨ Key Features

### 🧠 Theme-Based Analysis
Automatically categorizes reviews into relevant game themes:
- **Gameplay Mechanics** (controls, difficulty, progression)
- **Graphics & Performance** (visuals, FPS, optimization)
- **Story & Characters** (narrative, voice acting, pacing)
- **Audio Design** (music, sound effects, voice quality)
- **Multiplayer & Social** (matchmaking, community, balance)
- **Technical Issues** (bugs, crashes, compatibility)

### 📊 Actionable Insights Dashboard
- **What Players Love**: Positive sentiment summaries by theme
- **What Needs Fixing**: Critical issues and pain points
- **Theme Performance**: Like/dislike ratios for each game aspect
- **Sentiment Trends**: Track how player opinions change over time

### ⚡ High-Performance Processing
- **Distributed Computing**: Uses Dask for parallel processing across multiple workers
- **GPU Acceleration**: CUDA-optimized for high-throughput analysis
- **Smart Resource Management**: Automatically scales based on available hardware
- **Memory Efficient**: Handles datasets larger than available RAM

### 🔧 Developer-Friendly
- **Interactive Web Interface**: No coding required—upload data and get insights
- **CSV Export**: Easy integration with existing analytics workflows
- **Custom Themes**: Define your own game-specific categories
- **Batch Processing**: Analyze multiple games simultaneously

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd steamLensAI

# Install dependencies
pip install -r requirements.txt

# Launch the application
streamlit run app.py
```

### 2. Prepare Your Steam Data

**Option A: Use Existing Data**
- Export Steam review data in Parquet format
- Ensure data includes: `steam_appid`, `review`, `review_language`, `voted_up`

**Option B: Collect Fresh Data**
```bash
# Get Steam app IDs
python src/Step_1_collection_and_cleaning/getAppIds.py

# Collect reviews and metadata
python src/Step_1_collection_and_cleaning/getAppDetails.py

# Convert to optimized format
python src/Step_2_parquet_conversion/dask_json_to_parquet.py
```

### 3. Define Game Themes

Create a `game_themes.json` file with themes relevant to your game(s):

```json
{
  "391540": {
    "story": ["story", "plot", "narrative", "characters", "ending"],
    "gameplay": ["gameplay", "mechanics", "controls", "difficulty"],
    "music": ["music", "soundtrack", "audio", "sound"],
    "visuals": ["graphics", "art", "pixel art", "animation"]
  }
}
```

### 4. Analyze Your Game

1. **Upload**: Add your review data and theme definitions
2. **Process**: Let steamLensAI categorize and analyze reviews
3. **Insights**: Get actionable summaries for each theme
4. **Export**: Download findings for team collaboration

## 💡 Use Cases for Game Developers

### 🔧 **Post-Launch Analysis**
*"Our game just launched. What are players saying?"*

- Identify the most common complaints for immediate patches
- Understand which features players love most
- Prioritize bug fixes based on player impact
- Track sentiment changes after updates

### 📈 **Feature Development Planning**
*"What should we work on next?"*

- See which game aspects have the lowest satisfaction
- Understand player expectations for new content
- Validate feature concepts against player feedback
- Plan development roadmap based on player priorities

### 🎯 **Competitive Intelligence**
*"How do we compare to similar games?"*

- Analyze competitor reviews using the same themes
- Identify market gaps and opportunities
- Understand player expectations in your genre
- Benchmark your game's strengths and weaknesses

### 🚀 **Marketing Insights**
*"What should we highlight in our marketing?"*

- Find your game's most praised features
- Understand player language and terminology
- Identify review quotes for marketing materials
- Track community sentiment trends

## 📊 Example Analysis Output

### Counter-Strike 2 Analysis
```
🎯 Gameplay Mechanics
├── 👍 What Players Love: "Tight controls and precise shooting mechanics"
├── 👎 What Needs Fixing: "Anti-cheat system needs improvement"
└── 📈 Sentiment: 78% positive (45,231 reviews)

🖥️ Performance & Technical
├── 👍 What Players Love: "Smooth 144fps gameplay on modern hardware"
├── 👎 What Needs Fixing: "Frequent crashes and memory leaks"
└── 📈 Sentiment: 64% positive (32,109 reviews)
```

### Actionable Insights Generated:
1. **Priority Fix**: Focus on anti-cheat improvements (highest volume complaint)
2. **Marketing Asset**: Highlight precise shooting mechanics in promotions
3. **Technical Debt**: Address performance issues before next major update
4. **Community Communication**: Acknowledge cheating concerns publicly

## 🏗️ Architecture for Developers

### Processing Pipeline
```
Steam Reviews → Theme Classification → Sentiment Separation → AI Summarization → Insights Dashboard
```

### Key Components
- **Data Ingestion**: Efficient parquet file processing with validation
- **Theme Engine**: Semantic similarity matching using Sentence Transformers
- **Sentiment Analysis**: Automatic positive/negative review separation
- **Summarization**: GPU-accelerated text summarization with configurable length
- **Insights Engine**: Developer-focused summary generation

### Technology Stack
- **Backend**: Python, Dask, PyTorch
- **NLP**: Sentence Transformers, Transformers (Hugging Face)
- **UI**: Streamlit with real-time monitoring
- **Data**: PyArrow, Pandas for efficient data processing
- **Deployment**: Docker-ready, scales horizontally

## ⚙️ Configuration for Production

### Hardware Recommendations
- Will work on most hardware but a Nvidia GPU is required. The program check for the available resources and allocation is done automatically

### Performance Tuning
```python
# config/app_config.py
HARDWARE_CONFIG = {
    'worker_count': 8,              # Scale based on CPU cores
    'memory_per_worker': '4GB',     # Adjust for dataset size
    'gpu_batch_size': 256,          # Increase for better GPUs
    'max_summary_length': 300,      # Longer summaries for detailed insights
    'chunk_size': 800,              # Larger chunks for better context
}
```

## 📈 ROI for Game Development Teams

### Time Savings
- **Before**: Days manually reading reviews
- **After**: Minutes getting comprehensive insights
- **Impact**: 95%+ time reduction in player feedback analysis

### Better Decision Making
- **Data-Driven Patches**: Fix issues that actually matter to players
- **Feature Prioritization**: Focus development on high-impact areas
- **Community Relations**: Respond to player concerns proactively

### Competitive Advantage
- **Market Understanding**: Know what players want in your genre
- **Faster Response**: Address issues before competitors
- **Player Retention**: Improve satisfaction through targeted fixes

## 🛠️ Integration Options (Future Scope - Currently working on it)

### CI/CD Integration
```bash
# Automated analysis after each release
python analyze_reviews.py --game-id 12345 --since-date 2024-01-01
```

### API for Custom Workflows
```python
from steamLensAI import ReviewAnalyzer

analyzer = ReviewAnalyzer(config="game_themes.json")
insights = analyzer.analyze_game(app_id=12345)
print(insights.top_complaints)  # Most critical issues
print(insights.key_strengths)   # Marketing-ready highlights
```

### Slack/Discord Integration
Get automated reports sent to your development channels when sentiment changes significantly.

## 📋 Game Studio Requirements

### Minimum System Requirements
- **Python**: 3.8+
- **Memory**: 8GB RAM (16GB+ recommended)
- **GPU**: Required

### Data Requirements
- Steam review data in Parquet format
- Theme definitions relevant to your game
- Basic familiarity with data analysis workflows

## 🎯 Perfect For

### Indie Developers
- Understand player feedback without hiring data analysts
- Make informed decisions about limited development resources
- Compete with larger studios through better player understanding

### AA/AAA Studios
- Scale review analysis across multiple titles
- Integrate insights into existing development workflows
- Automate post-launch feedback monitoring

### Publishers
- Compare performance across portfolio games
- Identify successful patterns and common issues
- Make data-driven acquisition decisions

## 🔮 Roadmap

### Upcoming Features
- **Real-time Analysis**: Live Steam review monitoring
- **Sentiment Trending**: Track changes over time
- **Competitive Benchmarking**: Multi-game comparison dashboards
- **API Access**: Programmatic integration for development tools
- **Custom Models**: Train domain-specific sentiment models

### Community Features
- **Theme Library**: Shared theme definitions for common genres
- **Insight Templates**: Pre-built analysis workflows
- **Integration Plugins**: Connect with popular development tools

## 🤝 Contributing

We welcome contributions from game developers and data scientists:

1. **Feature Requests**: What insights would help your development process?
2. **Theme Definitions**: Share theme configurations for different game genres
3. **Performance Optimizations**: Help us handle even larger datasets
4. **Integration Examples**: Show how you use steamLensAI in your workflow

## 📞 Support

### For Game Developers
- **Documentation**: Comprehensive guides for non-technical users
- **Community Forum**: Connect with other developers using steamLensAI
- **Consulting**: Custom implementation for complex requirements

### Technical Support
- **GitHub Issues**: Bug reports and feature requests
- **Performance Tuning**: Optimization for large-scale analysis
- **Custom Deployment**: Enterprise and cloud deployment assistance

---

**Transform player feedback into development gold** 🏆

Built by developers, for developers. Stop guessing what players want—know exactly what to build next.

[Get Started](#-quick-start) | [View Examples](#-example-analysis-output) | [Join Community](#-contributing)
