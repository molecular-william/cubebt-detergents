#!/usr/bin/env python3
"""
Optimized Solubilization Database Scraper (Basic Speed Optimizations)
Optimized for speed without parallel processing or async programming
Uses Polars for high-performance data handling instead of pandas and optimized web drivers
"""

import time
import re
import argparse
from bs4 import BeautifulSoup
import polars as pl
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import sys


class OptimizedBasicScraper:
    def __init__(self, url="https://cube-biotech.com/solubilization-database"):
        self.url = url
        self.driver = None
        self.data = []
        
    def setup_driver(self):
        """Setup optimized driver (Chrome or Firefox) with enhanced performance options"""
        
        # Firefox optimization
        firefox_options = FirefoxOptions()
        firefox_options.add_argument("--headless")
        firefox_options.add_argument("--no-sandbox")
        firefox_options.add_argument("--disable-dev-shm-usage")
        firefox_options.add_argument("--disable-gpu")
        firefox_options.add_argument("--disable-extensions")
        firefox_options.add_argument("--disable-plugins")
        firefox_options.add_argument("--disable-images")
        # firefox_options.add_argument("--disable-javascript")
        firefox_options.add_argument("--disable-css")
        
        # Performance optimizations for Firefox
        # firefox_options.set_preference("browser.cache.disk.enable", False)
        # firefox_options.set_preference("browser.cache.memory.enable", False)
        # firefox_options.set_preference("network.http.use-cache", False)
        firefox_options.set_preference("general.useragent.override", 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0")
        
        try:
            self.driver = webdriver.Firefox(options=firefox_options)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(5)  # Reduced wait time
            return True
        except Exception as e:
            print(f"Error setting up Firefox driver: {e}")
            return False

    
    def select_experiment(self, experiment_choice="1"):
        """Optimized experiment selection with reduced wait times"""
        try:
            print(f"Selecting experiment: {experiment_choice}")
            
            # Optimized wait times
            select_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "select_experiment"))
            )
            
            select = Select(select_element)
            
            # Get available options more efficiently
            available_options = [opt.text.strip() for opt in select.options if opt.text.strip()]
            print(f"Available experiments: {len(available_options)} options")
            
            # Optimized selection logic
            try:
                if experiment_choice in ["1", "2"]:
                    select.select_by_value(experiment_choice)
                else:
                    # Fast text matching
                    for option in available_options:
                        if experiment_choice.lower() in option.lower():
                            select.select_by_visible_text(option)
                            break
                    else:
                        select.select_by_index(0)
            except Exception:
                select.select_by_index(0)
            
            # Reduced wait time after selection
            time.sleep(1)
            
            # Optimized table wait
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "datatable"))
            )
            
            return True
            
        except Exception as e:
            print(f"Error selecting experiment: {e}")
            return False
    
    def clean_tooltip_data_fast(self, tooltip_text):
        """Ultra-fast data cleaning with regex optimization"""
        if not tooltip_text or not tooltip_text.strip():
            return ""
        
        # Single regex operation for all cleanup
        cleaned = re.sub(r'\s+', ' ', tooltip_text.strip())
        return cleaned if cleaned else ""
    
    def extract_table_data_fast(self):
        """Optimized table data extraction with faster parsing"""
        try:
            # Optimized wait time
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "datatable"))
            )
            
            # Fast HTML parsing
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Optimized table finding
            table = soup.find('table', {'id': 'datatable'})
            if not table:
                return False
            
            # Fast header extraction
            headers = []
            thead = table.find('thead')
            if thead:
                headers = [th.get_text().strip() for th in thead.find_all('th')]
            
            # Fast body processing
            tbody = table.find('tbody')
            if not tbody:
                return False
            
            rows = tbody.find_all('tr')
            
            # Pre-compile regex for performance
            # rows_processed = 0
            
            for row in rows:
                cells = row.find_all('td')
                if not cells:
                    continue
                
                row_data = {}
                
                # Optimized cell processing
                for i, cell in enumerate(cells):
                    if i >= len(headers):
                        break
                    
                    header = headers[i]
                    
                    # Fast span processing
                    spans = cell.find_all('span', {'data-tooltip': True})
                    if spans:
                        # Get first tooltip data
                        tooltip_data = self.clean_tooltip_data_fast(spans[0].get('data-tooltip', ''))
                        if tooltip_data:
                            try:
                                row_data[header] = float(tooltip_data)
                            except:
                                row_data[header] = None  # No data
                    else:
                        # Fast text extraction
                        text = cell.get_text().strip()
                        if text:
                            row_data[header] = text
                
                if row_data:
                    self.data.append(row_data)
                    # rows_processed += 1
            
            return True
            
        except Exception as e:
            print(f"Error extracting table data: {e}")
            return False
    
    def go_to_next_page_fast(self):
        """Optimized pagination with faster navigation"""
        for attempt in range(10):
            try:
                # Optimized selectors for next page button
                next_selectors = [
                    "a.next",
                    "li.next a", 
                    ".paginate_button.next",
                    "[data-dt-idx='next']",
                    "a[aria-label='Next']"
                ]
                
                next_button = None
                for selector in next_selectors:
                    try:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if element.is_enabled() and "disabled" not in element.get_attribute("class"):
                            next_button = element
                            break
                    except NoSuchElementException:
                        continue
                
                if not next_button:
                    return False
                
                # Fast click and navigation
                self.driver.execute_script("arguments[0].click();", next_button)
                time.sleep(0.5)  # Reduced wait time
                
                # Optimized verification
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.ID, "datatable"))
                )
                
                return True
                
            except Exception as e:
                return False
    
    def scrape_all_pages_fast(self, experiment_choice="1", max_pages=50):
        """Ultra-optimized scraping with minimal overhead"""
        print(f"🚀 Starting optimized scraping: {self.url}")
        
        if not self.setup_driver():
            return False
        
        try:
            # Fast page loading
            print("📡 Loading page...")
            self.driver.get(self.url)
            time.sleep(0.5)  # Reduced initial wait
            
            # Fast experiment selection
            if not self.select_experiment(experiment_choice):
                print("⚠️ Failed to select experiment, continuing...")
            
            page_count = 1
            
            while page_count <= max_pages:
                # Fast data extraction
                if not self.extract_table_data_fast():
                    print(f"❌ Failed to extract data from page {page_count}")
                    break
                
                # Fast pagination
                if not self.go_to_next_page_fast():
                    print(f"🏁 No more pages or failed navigation after page {page_count}")
                    break
                
                page_count += 1
                
                # Progress indicator every 10 pages
                if page_count % 10 == 0:
                    print(f"✅ Progress: {page_count-1} pages processed, {len(self.data)} records collected")
            
            print(f"\n🎉 Scraping completed!")
            print(f"📈 Total records: {len(self.data)}")
            return True
            
        except Exception as e:
            print(f"💥 Error during scraping: {e}")
            return False
        finally:
            if self.driver:
                self.driver.quit()
    
    def save_to_csv_optimized(self, filename="optimized_data.csv"):
        """Ultra-optimized CSV writing using Polars for maximum performance"""
        if not self.data:
            print("❌ No data to save")
            return False
        
        try:
            print(f"💾 Saving {len(self.data)} records to {filename}...")
            
            # Convert to Polars DataFrame for high-performance operations
            # Polars can handle missing columns automatically and provides much better performance
            df = pl.DataFrame(self.data)
            
            # Save to CSV with optimized settings
            df.write_csv(
                filename, 
                include_header=True, 
                separator=',',
                quote_style='non_numeric',
                float_precision=None
            )
            
            print(f"✅ Data saved successfully with Polars!")
            print(f"📁 File: {filename}")
            print(f"📊 Records: {len(df):,}")
            print(f"📋 Columns: {len(df.columns)}")
            
            # Enhanced data summary with Polars
            print(f"\n📈 Polars Data Summary:")
            print(f"   Records: {len(df):,}")
            print(f"   Columns: {len(df.columns)}")
            print(f"   Memory usage: ~{df.estimated_size('mb'):.1f} MB")
            
            # Show column names with data types
            print(f"   Sample columns: {df.columns[:5]}{'...' if len(df.columns) > 5 else ''}")
            
            # Show data types for key columns
            if df.columns:
                key_cols = df.columns[:3]  # Show first 3 column types
                print(f"   Sample dtypes: {[f'{col}: {df[col].dtype}' for col in key_cols]}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error saving to CSV with Polars: {e}")
            return False


def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(description='Optimized Solubilization Database Scraper (Basic)')
    parser.add_argument('--experiment', '-e', default='1', help='Experiment to scrape (1, 2, or name)')
    parser.add_argument('--output', '-o', default='optimized_data.csv', help='Output CSV filename')
    parser.add_argument('--max-pages', '-p', type=int, default=100, help='Maximum pages to scrape')
    
    args = parser.parse_args()
    
    scraper = OptimizedBasicScraper()
    
    print("=" * 70)
    print("🚀 OPTIMIZED SOLUBILIZATION DATABASE SCRAPER (BASIC)")
    print("=" * 70)
    print(f"🎯 Experiment: {args.experiment}")
    print(f"📁 Output: {args.output}")
    print(f"📄 Max pages: {args.max_pages}")
    print("🌐 Driver: Firefox")
    print("=" * 70)
    
    # Start scraping
    start_time = time.time()
    
    if scraper.scrape_all_pages_fast(args.experiment, args.max_pages):
        if scraper.save_to_csv_optimized(args.output):
            end_time = time.time()
            duration = end_time - start_time
            print(f"\n🎉 SUCCESS! Scraping completed in {duration:.2f} seconds")
            print(f"📁 Data saved as: {args.output}")
            print(f"⚡ Average speed: {len(scraper.data)/duration:.1f} records/second")
        else:
            print("\n❌ Scraping completed but failed to save data")
    else:
        print("\n❌ Scraping failed")
        sys.exit(1)


if __name__ == "__main__":
    # Show usage if no arguments
    if len(sys.argv) == 1:
        print("🚀 Optimized Solubilization Database Scraper (Basic)")
        print("=" * 60)
        print("\n📋 Usage examples:")
        print("  python optimized_scraper_basic.py --experiment 1")
        print("  python optimized_scraper_basic.py --experiment 2 --output data.csv")
        print("  python optimized_scraper_basic.py --experiment 1 --max-pages 50 --driver firefox")
        print("\n🔧 Options:")
        print("  --driver chrome|firefox  Choose web driver (default: chrome)")
        print("  --max-pages N           Maximum pages to scrape (default: 100)")
        print("\n💡 Performance tips:")
        print("  • Firefox often performs better for simple scraping")
        print("  • Chrome has better JavaScript support if needed")
        print("  • Use --max-pages to limit scraping scope")
        sys.exit(0)
    
    main()