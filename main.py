from src.etl import run_etl_pipeline

def main():
    csv_path = "data/ad_click_dataset.csv"  
    run_etl_pipeline(csv_path)

if __name__ == "__main__":
    main()
