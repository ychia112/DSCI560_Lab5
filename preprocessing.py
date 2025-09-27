import os, re, json, logging, argparse
from typing import List, Dict, Tuple
import mysql.connector as mysql
from dotenv import load_dotenv
import html
from bs4 import BeautifulSoup
import requests
from PIL import Image
from io import BytesIO
import pytesseract
import numpy as np
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

load_dotenv()

class RedditPreprocessor:
    """
    Focused Reddit Preprocessing & Feature Engineering
    
    Core Responsibilities:
    1. HTML tags cleaning 
    2. OCR text extraction from images
    3. Document embeddings generation 
    4. Enhanced text cleaning for embeddings
    
    Preserving data collection results:
    - is_ad (advertisement detection already done)
    - keywords (simple keywords already extracted)
    """
    
    def __init__(self):
        self.logger = self._setup_logging()
        
        # Regex patterns focused on text cleaning
        self.cleaning_patterns = {
            'html_tags': re.compile(r'<[^>]+>'),
            'urls': re.compile(r'http[s]?://\S+'),
            'reddit_users': re.compile(r'/?u/[A-Za-z0-9_-]+'),
            'reddit_subs': re.compile(r'/?r/[A-Za-z0-9_-]+'),
            'emails': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            'extra_spaces': re.compile(r'\s+'),
            'special_chars': re.compile(r'[^\w\s]')  # Conservative special char cleaning
        }
        
        self.logger.info("Initialized focused RedditPreprocessor")

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def db_connection(self):
        return mysql.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB", "reddit_db"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            autocommit=True,
        )

    def clean_html_content(self, text: str) -> str:
        """
        Remove HTML tags and decode HTML entities
        This was NOT done in data collection phase
        """
        if not text:
            return ""
        
        # Decode HTML entities first
        text = html.unescape(text)
        
        # Remove HTML tags using BeautifulSoup for accuracy
        try:
            soup = BeautifulSoup(text, 'html.parser')
            text = soup.get_text(separator=' ')
        except:
            # Fallback to regex if BeautifulSoup fails
            text = self.cleaning_patterns['html_tags'].sub(' ', text)
        
        return text

    def extract_text_from_images(self, url: str) -> str:
        """
        Extract text from images using pytesseract OCR
        Lab requirement: Use OCR for embedded images
        
        Args:
            url: Image URL to process
            
        Returns:
            Extracted text or empty string
        """
        try:
            # Check if URL points to an image
            image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
            if not any(url.lower().endswith(ext) for ext in image_extensions):
                return ""
            
            # Download image with timeout and proper headers
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; RedditScraper/1.0)'}
            response = requests.get(url, timeout=15, headers=headers)
            
            if response.status_code != 200:
                return ""
            
            # Open and process image
            image = Image.open(BytesIO(response.content))
            
            # Convert to RGB if needed for OCR
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Extract text using OCR
            extracted_text = pytesseract.image_to_string(image, lang='eng')
            
            if extracted_text:
                # Clean OCR output
                extracted_text = re.sub(r'\n+', ' ', extracted_text)
                extracted_text = re.sub(r'\s+', ' ', extracted_text).strip()
                
                # Filter very short results (likely noise)
                if len(extracted_text) >= 5:
                    return extracted_text
            
        except Exception as e:
            self.logger.debug(f"OCR failed for {url}: {str(e)}")
        
        return ""

    def enhanced_text_cleaning(self, text: str) -> str:
        """
        Enhanced text cleaning for better embeddings
        Builds upon basic cleaning from data collection
        """
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove HTML content (not done in data collection)
        text = self.clean_html_content(text)
        
        # Replace URLs with placeholder to preserve context
        text = self.cleaning_patterns['urls'].sub(' [URL] ', text)
        
        # Anonymize users and subreddits (enhancing data collection anonymization)
        text = self.cleaning_patterns['reddit_users'].sub(' [USER] ', text)
        text = self.cleaning_patterns['reddit_subs'].sub(' [SUBREDDIT] ', text)
        
        # Remove email addresses for privacy
        text = self.cleaning_patterns['emails'].sub(' [EMAIL] ', text)
        
        # Conservative special character removal (preserve sentence structure)
        text = self.cleaning_patterns['special_chars'].sub(' ', text)
        
        # Normalize whitespace
        text = self.cleaning_patterns['extra_spaces'].sub(' ', text)
        text = text.strip()
        
        return text

    def generate_doc2vec_embeddings(self, documents: List[str]) -> Tuple[List[List[float]], Doc2Vec]:
        """
        Generate document embeddings using Doc2Vec
        Core feature engineering task as per lab requirements
        
        Args:
            documents: List of cleaned text documents
            
        Returns:
            Tuple of (embeddings_list, trained_model)
        """
        try:
            # Filter valid documents for training
            valid_docs = []
            doc_indices = []
            
            for i, doc in enumerate(documents):
                if doc and doc.strip() and len(doc.strip()) > 10:
                    valid_docs.append(doc.strip())
                    doc_indices.append(i)
            
            if len(valid_docs) < 2:
                self.logger.warning("Not enough valid documents for Doc2Vec training")
                return [[0.0] * 100 for _ in documents], None
            
            # Create tagged documents for training
            tagged_docs = [
                TaggedDocument(words=doc.lower().split(), tags=[str(i)]) 
                for i, doc in enumerate(valid_docs)
            ]
            
            # Train Doc2Vec model with lab-recommended parameters
            self.logger.info(f"Training Doc2Vec on {len(tagged_docs)} documents...")
            
            model = Doc2Vec(
                tagged_docs,
                vector_size=100,      # 100-dimensional vectors
                window=5,             # Context window size
                min_count=2,          # Ignore words with freq < 2
                workers=4,            # CPU cores to use
                epochs=20,            # Training iterations
                dm=1,                 # Distributed Memory model
                alpha=0.025,          # Initial learning rate
                min_alpha=0.00025,    # Final learning rate
                sample=1e-4           # Threshold for word downsampling
            )
            
            # Generate embeddings for all documents
            embeddings = []
            valid_idx = 0
            
            for i, doc in enumerate(documents):
                if i in doc_indices:
                    try:
                        # Get trained embedding
                        embedding = model.dv[str(valid_idx)]
                        embeddings.append(embedding.tolist())
                        valid_idx += 1
                    except (KeyError, IndexError):
                        # Infer embedding for unseen document
                        words = doc.lower().split()
                        embedding = model.infer_vector(words)
                        embeddings.append(embedding.tolist())
                else:
                    # Zero vector for invalid documents
                    embeddings.append([0.0] * 100)
            
            self.logger.info(f"Generated {len(embeddings)} Doc2Vec embeddings (100D)")
            return embeddings, model
            
        except Exception as e:
            self.logger.error(f"Doc2Vec embedding generation failed: {e}")
            return [[0.0] * 100 for _ in documents], None

    def process_posts_batch(self, batch_size: int = 50) -> int:
        """
        Process a batch of posts from the database
        Focus only on adding new features, preserve existing data
        
        Returns:
            Number of posts processed
        """
        conn = self.db_connection()
        cursor = conn.cursor()
        
        # Get unprocessed posts (missing embeddings or OCR text)
        query = """
        SELECT id, title, selftext, url
        FROM reddit_posts 
        WHERE embedding IS NULL OR ocr_text IS NULL
        LIMIT %s
        """
        
        cursor.execute(query, (batch_size,))
        posts = cursor.fetchall()
        
        if not posts:
            self.logger.info("No posts found to process")
            cursor.close()
            conn.close()
            return 0
        
        self.logger.info(f"Processing {len(posts)} posts...")
        
        # Process each post
        processed_data = []
        texts_for_embedding = []
        
        for post in posts:
            post_id, title, selftext, url = post
            
            # Combine title and content
            full_text = f"{title or ''} {selftext or ''}".strip()
            
            if not full_text:
                # Still process for OCR even if no text content
                processed_data.append({
                    'id': post_id,
                    'cleaned_text': "",
                    'ocr_text': self.extract_text_from_images(url) if url else ""
                })
                texts_for_embedding.append("empty")
                continue
            
            # Enhanced text cleaning for embeddings
            cleaned_text = self.enhanced_text_cleaning(full_text)
            
            # OCR text extraction if URL points to image
            ocr_text = ""
            if url:
                ocr_text = self.extract_text_from_images(url)
                if ocr_text:
                    # Add OCR text to cleaned content for embeddings
                    ocr_cleaned = self.enhanced_text_cleaning(ocr_text)
                    cleaned_text = f"{cleaned_text} {ocr_cleaned}".strip()
            
            processed_data.append({
                'id': post_id,
                'cleaned_text': cleaned_text,
                'ocr_text': ocr_text
            })
            
            # Prepare text for embedding generation
            embedding_text = cleaned_text if cleaned_text.strip() else "empty"
            texts_for_embedding.append(embedding_text)
        
        # Generate embeddings for all texts
        if texts_for_embedding:
            embeddings, model = self.generate_doc2vec_embeddings(texts_for_embedding)
            
            # Update database with new features only
            self._update_database_batch(processed_data, embeddings)
        
        cursor.close()
        conn.close()
        return len(processed_data)

    def _update_database_batch(self, processed_data: List[Dict], embeddings: List[List[float]]):
        """
        Update database with new features only
        Preserve existing keywords and is_ad from data collection
        """
        conn = self.db_connection()
        cursor = conn.cursor()
        
        # Update only new columns: clean_text, embedding, ocr_text
        # Do NOT touch keywords or is_ad
        update_query = """
        UPDATE reddit_posts 
        SET clean_text = %s, embedding = %s, ocr_text = %s
        WHERE id = %s
        """
        
        for i, data in enumerate(processed_data):
            embedding_json = json.dumps(embeddings[i]) if i < len(embeddings) else None
            
            cursor.execute(update_query, (
                data['cleaned_text'],
                embedding_json,
                data['ocr_text'],
                data['id']
            ))
        
        conn.commit()
        self.logger.info(f"Updated {len(processed_data)} posts with new features")
        
        cursor.close()
        conn.close()

    def process_all_posts(self, batch_size: int = 50):
        """Process all unprocessed posts in the database"""
        self.logger.info("Starting focused preprocessing (HTML cleaning + OCR + embeddings)...")
        
        total_processed = 0
        batch_num = 1
        
        while True:
            self.logger.info(f"Processing batch {batch_num}...")
            processed_count = self.process_posts_batch(batch_size)
            
            if processed_count == 0:
                break
            
            total_processed += processed_count
            batch_num += 1
            
            # Brief pause between batches
            import time
            time.sleep(1)
        
        self.logger.info(f"Focused preprocessing completed! Total processed: {total_processed}")

    def get_preprocessing_stats(self) -> Dict:
        """Get preprocessing statistics"""
        conn = self.db_connection()
        cursor = conn.cursor()
        
        # Get various counts
        cursor.execute("SELECT COUNT(*) FROM reddit_posts")
        total_posts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reddit_posts WHERE embedding IS NOT NULL")
        posts_with_embeddings = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reddit_posts WHERE ocr_text IS NOT NULL AND ocr_text != ''")
        posts_with_ocr = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reddit_posts WHERE keywords IS NOT NULL AND keywords != '[]'")
        posts_with_keywords = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM reddit_posts WHERE clean_text IS NOT NULL AND clean_text != ''")
        posts_with_clean_text = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        completion_rate = (posts_with_embeddings / total_posts * 100) if total_posts > 0 else 0
        
        return {
            'total_posts': total_posts,
            'posts_with_embeddings': posts_with_embeddings,
            'posts_with_ocr': posts_with_ocr,
            'posts_with_keywords': posts_with_keywords,
            'posts_with_clean_text': posts_with_clean_text,
            'completion_percentage': completion_rate
        }


def main():
    parser = argparse.ArgumentParser(description='Focused Reddit Preprocessing & Feature Engineering')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='Batch size for processing')
    parser.add_argument('--stats', action='store_true',
                       help='Show preprocessing statistics')
    
    args = parser.parse_args()
    
    preprocessor = RedditPreprocessor()
    
    if args.stats:
        stats = preprocessor.get_preprocessing_stats()
        print(f"\n=== Focused Preprocessing Statistics ===")
        print(f"Total posts: {stats['total_posts']}")
        print(f"Posts with embeddings: {stats['posts_with_embeddings']}")
        print(f"Posts with OCR text: {stats['posts_with_ocr']}")
        print(f"Posts with keywords (from data collection): {stats['posts_with_keywords']}")
        print(f"Posts with enhanced clean text: {stats['posts_with_clean_text']}")
        print(f"Completion: {stats['completion_percentage']:.1f}%")
    else:
        preprocessor.process_all_posts(batch_size=args.batch_size)


# if __name__ == "__main__":
#     main()