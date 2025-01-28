import pandas as pd
import tempfile
from datetime import datetime
from typing import List, Dict

class DataExporter:
    """Handles data export to CSV and Excel formats"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def prepare_data_for_export(self, listings: List[Dict]) -> pd.DataFrame:
        """Prepare listings data for export by flattening and cleaning"""
        # Define columns we want to export
        export_columns = [
            'listing_id', 'source_website', 'listing_type', 'property_type',
            'price', 'currency', 'rooms', 'area', 'floor', 'total_floors',
            'district', 'metro_station', 'address', 'location',
            'latitude', 'longitude', 'contact_type', 'contact_phone',
            'views_count', 'has_repair', 'listing_date', 'updated_at'
        ]
        
        # Convert listings to DataFrame
        df = pd.DataFrame(listings)
        
        # Ensure all expected columns exist
        for col in export_columns:
            if col not in df.columns:
                df[col] = None
        
        # Select and order columns
        df = df[export_columns]
        
        # Clean and format data
        df['listing_date'] = pd.to_datetime(df['listing_date']).dt.strftime('%Y-%m-%d')
        df['updated_at'] = pd.to_datetime(df['updated_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['price'] = df['price'].fillna(0).astype(float).round(2)
        df['area'] = df['area'].fillna(0).astype(float).round(2)
        
        return df
    
    def create_excel_report(self, listings: List[Dict], scraper_stats: Dict, db_stats: Dict) -> str:
        """Create Excel report with multiple sheets for data and statistics"""
        try:
            # Create temp file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            temp_file = tempfile.NamedTemporaryFile(
                prefix=f'real_estate_report_{timestamp}_',
                suffix='.xlsx',
                delete=False
            )
            
            # Create Excel writer
            with pd.ExcelWriter(temp_file.name, engine='xlsxwriter') as writer:
                # Main data sheet
                df_listings = self.prepare_data_for_export(listings)
                df_listings.to_excel(writer, sheet_name='Listings', index=False)
                
                # Statistics sheets
                self._add_stats_sheet(writer, scraper_stats, db_stats)
                
                # Format sheets
                self._format_excel_sheets(writer, len(df_listings))
            
            return temp_file.name
            
        except Exception as e:
            self.logger.error(f"Error creating Excel report: {str(e)}")
            raise

    def create_csv_report(self, listings: List[Dict]) -> str:
        """Create CSV report with just the listings data"""
        try:
            # Create temp file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            temp_file = tempfile.NamedTemporaryFile(
                prefix=f'real_estate_report_{timestamp}_',
                suffix='.csv',
                delete=False
            )
            
            # Convert and save data
            df = self.prepare_data_for_export(listings)
            df.to_csv(temp_file.name, index=False, encoding='utf-8-sig')
            
            return temp_file.name
            
        except Exception as e:
            self.logger.error(f"Error creating CSV report: {str(e)}")
            raise

    def _add_stats_sheet(self, writer: pd.ExcelWriter, scraper_stats: Dict, db_stats: Dict):
        """Add statistics sheets to Excel file"""
        # Scraper Statistics
        scraper_data = []
        for website in scraper_stats['success_count'].keys():
            scraper_data.append({
                'Website': website,
                'Successful': scraper_stats['success_count'][website],
                'Errors': scraper_stats['error_count'][website],
                'Success Rate': f"{(scraper_stats['success_count'][website] / (scraper_stats['success_count'][website] + scraper_stats['error_count'][website]) * 100):.1f}%"
            })
        
        df_scraper = pd.DataFrame(scraper_data)
        df_scraper.to_excel(writer, sheet_name='Scraper Stats', index=False)
        
        # Database Statistics
        db_data = [{
            'Metric': 'New Listings',
            'Count': db_stats['successful_inserts']
        }, {
            'Metric': 'Updated Listings',
            'Count': db_stats['successful_updates']
        }, {
            'Metric': 'Failed Operations',
            'Count': db_stats['failed']
        }]
        
        # Add field updates
        for field, count in db_stats['updated_fields'].items():
            db_data.append({
                'Metric': f'Updated {field}',
                'Count': count
            })
        
        df_db = pd.DataFrame(db_data)
        df_db.to_excel(writer, sheet_name='Database Stats', index=False)

    def _format_excel_sheets(self, writer: pd.ExcelWriter, data_rows: int):
        """Apply formatting to Excel sheets"""
        workbook = writer.book
        
        # Format for Listings sheet
        worksheet = writer.sheets['Listings']
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D3D3D3',
            'border': 1
        })
        
        # Apply header format
        for col_num, value in enumerate(df_listings.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Auto-adjust columns width
        for column in df_listings:
            column_length = max(
                df_listings[column].astype(str).apply(len).max(),
                len(column)
            )
            col_idx = df_listings.columns.get_loc(column)
            worksheet.set_column(col_idx, col_idx, column_length + 2)
            
        # Add totals row
        worksheet.write(
            data_rows + 2, 0, 
            'Total Listings:', 
            workbook.add_format({'bold': True})
        )
        worksheet.write(
            data_rows + 2, 1, 
            data_rows,
            workbook.add_format({'bold': True})
        )