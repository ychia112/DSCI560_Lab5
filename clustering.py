import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np


def cluster_messages(embeddings, messages, n_clusters=5, random_state=42):
    """Cluster the embeddings and return labels and cluster keywords."""
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
    labels = kmeans.fit_predict(embeddings)

    # Use TF-IDF to extract keywords
    vectorizer = TfidfVectorizer(stop_words="english", max_features=2000)
    tfidf = vectorizer.fit_transform(messages)
    terms = vectorizer.get_feature_names_out()
    
    keywords = {}
    
    for cid in range(n_clusters):
        idxs = idxs = np.where(labels == cid)[0]
        
        if len(idxs) == 0:
            keywords[cid] = []
            continue
        
        mean_vec = tfidf[idxs].mean(axis=0)
        top_idx = np.array(mean_vec).ravel().argsort()[-8:][::-1]
        keywords[cid] = [terms[i] for i in top_idx]

    return labels, keywords



def get_representative_posts(embeddings, labels, ids, random_state=42):
    """Findout the representative message that closest to the centroid in each cluster"""
    k = len(np.unique(labels))
    km = KMeans(n_clusters=k, random_state=random_state).fit(embeddings)
    reps = {}
    
    for cid in range(k):
        idxs = np.where(labels == cid)[0]
        if len(idxs) == 0:
            reps[cid] = None
            continue
        center = km.cluster_centers_[cid]
        closest = idxs[np.argmin(np.linalg.norm(embeddings[idxs] - center, axis=1))]
        reps[cid] = ids[closest]
    
    return reps

def visualize_clusters(embeddings, labels, keywords, filename="clusters.png"):
    """Turn embeddings into 2D with PCA, and plot the scatter plot."""
    pca = PCA(n_components=2)
    reduced = pca.fit_transform(embeddings)
    
    df = pd.DataFrame({
        "x": reduced[:, 0],
        "y": reduced[:, 1],
        "cluster": labels
    })
    
    plt.figure(figsize=(8, 6))
    
    cluster_names = {cid: ", ".join(kws[:3]) for cid, kws in keywords.items()}
    df["cluster_name"] = df["cluster"].map(cluster_names)
    
    sns.scatterplot(
        data=df, x="x", y="y", hue="cluster_name",
        palette="Set2", s=50, alpha=0.85, edgecolor='none'
    )
    plt.title("Message Clusters", fontsize=14)
    plt.legend(title="Cluster", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    
    plt.savefig(filename)
    plt.close()