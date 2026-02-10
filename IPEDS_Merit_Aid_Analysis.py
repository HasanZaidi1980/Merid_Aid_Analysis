import pandas as pd
import numpy as np
import os
import plotly.graph_objects as go
from typing import Dict, Any, Tuple

# --- 1. CONFIGURATION ---
# IMPORTANT: Replace this with the actual path to your unzipped IPEDS files.
IPEDS_BASE_PATH = r"C:\Users\amatu\Downloads\Phase_3"
OUTPUT_FILE = "final_merit_college_rankings.csv"

# List of ESSENTIAL FILES for the Merit Aid model: (8 files)
ESSENTIAL_FILES = {
    'hd': 'HD2022.csv',
    'sfa_p1': 'SFA2122_P1.csv',
    'sfa_p2': 'SFA2122_P2.csv',
    'ic': 'IC2022_AY.csv',
    'adm_sat': 'ADM2022.csv',          
    'adm_rate': 'DRVADM2022.csv',       
    'gr': 'DRVGR2022.csv',             
    'mission': 'IC2022Mission.csv'  
}

# --- 2. ETL STAGE: EXTRACTION & INITIAL FILTERING ---

def load_and_clean_data(base_path: str, files_map: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    """Loads necessary IPEDS tables, cleans special values, and filters for 4-year degree-granting colleges."""

    data: Dict[str, pd.DataFrame] = {}
    
    # 2.1 Load Core Tables
    for key, filename in files_map.items():
        try:
            data[key] = pd.read_csv(os.path.join(base_path, filename), encoding='latin-1', low_memory=False)
        except FileNotFoundError as e:
            print(f"Error: Required file not found: {filename}. Please ensure this file exists in the directory.")
            return None

    # 2.2 Standard IPEDS Missing/Special Value Handling (e.g., -1, -2, -9)
    for df_name in ['sfa_p1', 'sfa_p2', 'ic', 'adm_sat', 'adm_rate', 'gr']:
        if df_name in data:
            data[df_name].replace([-1, -2, -9], np.nan, inplace=True)

    # 2.3 Initial Filtering: Target 4-year, degree-granting institutions
    hd = data['hd']
    
    # Use column 'HDEGOFR1' for Highest Degree Offered
    try:
        hd_filtered = hd[
            (hd['ICLEVEL'] == 1) &          
            (hd['HDEGOFR1'] >= 3) &           
            (hd['UNITID'].notna())
        ][['UNITID', 'INSTNM', 'CONTROL']]
        
        print(f"Successfully used 'HDEGOFR1' to filter for 4-year degree-granting institutions.")
        
    except KeyError as e:
        print(f"\n--- CRITICAL FILTER ERROR ---")
        print(f"KeyError: {e} not found in HD2022.csv. Please inspect the HD2022 columns.")
        return None
    
    # Return all 8 necessary dataframes
    return hd_filtered, data['sfa_p1'], data['sfa_p2'], data['ic'], data['adm_sat'], data['adm_rate'], data['gr'], data['mission']

# --- 3. ETL STAGE: TRANSFORMATION & METRIC CALCULATION ---

def calculate_metrics(hd, sfa_p1, sfa_p2, ic, adm_sat, adm_rate, gr, mission):
    """Joins dataframes and calculates the Merit Generosity Index (MGI)."""

    # 3.1 Combine SFA data (P1 and P2)
    sfa_data_merged = sfa_p1[['UNITID', 'IGRNT_A']].merge(
        sfa_p2[['UNITID', 'NPT442']], 
        on='UNITID',
        how='inner'
    )
    
    # 3.2 Combine Admissions Data 
    try:
        adm_sat_data = adm_sat[['UNITID', 'SATVR75', 'SATMT75']]
        adm_rate_data = adm_rate[['UNITID', 'DVADM01']] 
        
        adm_data_merged = adm_sat_data.merge(
            adm_rate_data,
            on='UNITID',
            how='left'
        )
    except KeyError as e:
        print(f"\n--- CRITICAL ADMISSIONS ERROR ---")
        print(f"KeyError: Missing required column {e}. Columns checked: ADM2022 needs SATVR75, SATMT75. DRVADM2022 needs DVADM01.")
        return pd.DataFrame() 

    # 3.3 Select Remaining Key Variables 
    ic_data = ic[['UNITID', 'TUITION2']]
    
    # Use confirmed column GBA4RTT for 4-year Graduation Rate
    try:
        gr_data = gr[['UNITID', 'GBA4RTT']] 
    except KeyError as e:
        print(f"\n--- CRITICAL GRADUATION RATE ERROR ---")
        print(f"KeyError: Missing required column {e}. DRVGR2022.csv must contain 'GBA4RTT'.")
        return pd.DataFrame() 
        
    # FIX: Use confirmed column 'mission' and rename 'unitid' to 'UNITID'
    try:
        mission_data = mission[['unitid', 'mission']].rename(columns={'unitid': 'UNITID'}) # <--- FIX APPLIED HERE
    except KeyError as e:
        print(f"\n--- CRITICAL MISSION ERROR ---")
        print(f"KeyError: Missing required column {e}. IC2022Mission.csv must contain 'unitid' and 'mission'.")
        return pd.DataFrame() 

    # 3.4 Merge All Tables (HD -> IC -> SFA -> ADM -> GR -> MISSION)
    merged_df = hd.merge(ic_data, on='UNITID', how='left')
    merged_df = merged_df.merge(sfa_data_merged, on='UNITID', how='left')
    merged_df = merged_df.merge(adm_data_merged, on='UNITID', how='left') 
    merged_df = merged_df.merge(gr_data, on='UNITID', how='left')
    merged_df = merged_df.merge(mission_data, on='UNITID', how='left')

    # Drop rows where essential financial data is missing
    merged_df.dropna(subset=['TUITION2', 'IGRNT_A', 'NPT442'], inplace=True)
    
    # 3.5 Core Metric Calculation: Merit Generosity Index (MGI)
    merged_df['MGI'] = merged_df['IGRNT_A'] / merged_df['TUITION2']
    
    # Fill NaN values in 'GBA4RTT' (Graduation Rate) with the median for ranking stability
    merged_df['GBA4RTT'].fillna(merged_df['GBA4RTT'].median(), inplace=True)
    
    # 3.6 Affordability Score: Net Price % of Tuition (Low is better)
    merged_df['NET_PRICE_RATIO'] = merged_df['NPT442'] / merged_df['TUITION2']

    # Rename for readability
    merged_df.rename(columns={
        'TUITION2': 'Sticker_Price',
        'IGRNT_A': 'Avg_Inst_Grant',
        'NPT442': 'Net_Price_MidClass', 
        'GBA4RTT': 'Graduation_Rate_4yr',
        'DVADM01': 'Admissions_Rate',
        'mission': 'MISSION' # Renaming 'mission' to 'MISSION' for output consistency
    }, inplace=True)
    
    # Final data type clean up (Grad Rate/Admissions Rate are percentages, convert to 0-1 scale)
    merged_df['Graduation_Rate_4yr'] = merged_df['Graduation_Rate_4yr'] / 100 
    merged_df['Admissions_Rate'] = merged_df['Admissions_Rate'] / 100

    return merged_df

# --- 4. INSIGHTS STAGE: RANKING & FINAL FILTERING ---

def generate_insights(df):
    """Applies final filtering and creates the final ranked list."""

    final_df = df.copy()
    
    # Filter 1: Must have a Net Price for the middle class below a target threshold ($25,000)
    final_df = final_df[final_df['Net_Price_MidClass'] <= 25000].copy()
    
    # Filter 2: Must have a competitive MGI (top 50% of the original dataset's MGI)
    mgi_threshold = df['MGI'].quantile(0.5) 
    final_df = final_df[final_df['MGI'] >= mgi_threshold].copy()
    
    # Ensure minimum 10 colleges are included (if too few, relax the net price filter)
    if len(final_df) < 10:
         print(f"Warning: Only {len(final_df)} colleges found. Relaxing Net Price filter to top 20% of MGI.")
         final_df = df[
             (df['Net_Price_MidClass'] <= df['Net_Price_MidClass'].quantile(0.3)) & 
             (df['MGI'] >= df['MGI'].quantile(0.8)) # Target top 20% of MGI
         ].copy()
    
    # 4.2 Ranking: Composite Score (MGI and Quality vs. Cost)
    final_df['Composite_Score'] = (final_df['MGI'] + final_df['Graduation_Rate_4yr']) / final_df['NET_PRICE_RATIO']
    
    final_df = final_df.sort_values(by='Composite_Score', ascending=False)
    
    # Select final columns for the client report
    final_report = final_df[[
        'UNITID', 'INSTNM', 'CONTROL', 'Sticker_Price', 'Avg_Inst_Grant', 
        'Net_Price_MidClass', 'MGI', 'Graduation_Rate_4yr', 'Admissions_Rate', 
        'SATVR75', 'SATMT75', 'Composite_Score', 'MISSION' 
    ]].head(20).reset_index(drop=True) # Show top 20

    return final_report

# --- 5. VISUALIZATION STAGE: RARE GRAPHS (Final Presentation) ---
# (Visualization functions remain the same)

def create_visualizations(report_df):
    """Generates the two required innovative visualizations using the final ranked data."""
    
    # Use the top 10 colleges for clear visualization
    vis_df = report_df.head(10).copy()

    # --- VISUALIZATION 1: The "Discount Gap" Dumbbell Chart ---
    fig_dumbbell = go.Figure()

    for i in range(len(vis_df)):
        fig_dumbbell.add_trace(go.Scatter(
            x=[vis_df['Net_Price_MidClass'][i], vis_df['Sticker_Price'][i]],
            y=[vis_df['INSTNM'][i], vis_df['INSTNM'][i]],
            mode='lines',
            line=dict(color='gray', width=2),
            showlegend=False
        ))

    fig_dumbbell.add_trace(go.Scatter(
            x=vis_df['Net_Price_MidClass'],
            y=vis_df['INSTNM'],
            mode='markers',
            name='Net Price (Middle Class)',
            marker=dict(color='green', size=12)
        ))

    fig_dumbbell.add_trace(go.Scatter(
            x=vis_df['Sticker_Price'],
            y=vis_df['INSTNM'],
            mode='markers',
            name='Sticker Price',
            marker=dict(color='red', size=12)
        ))

    fig_dumbbell.update_layout(
        title="1. The 'Discount Gap': Sticker Price vs. What You Actually Pay",
        xaxis_title="Cost ($)",
        yaxis_title="Institution",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    # --- VISUALIZATION 2: Parallel Coordinates Plot ---
    fig_parcoords = go.Figure(data=
        go.Parcoords(
            line = dict(color = vis_df['Composite_Score'], 
                       colorscale = 'Tealrose',
                       showscale = True,
                       cmin = vis_df['Composite_Score'].min(),
                       cmax = vis_df['Composite_Score'].max()),
            dimensions = list([
                dict(range = [vis_df['Sticker_Price'].min(), vis_df['Sticker_Price'].max()],
                     label = 'Sticker Price', values = vis_df['Sticker_Price']),
                dict(range = [vis_df['Avg_Inst_Grant'].min(), vis_df['Avg_Inst_Grant'].max()],
                     label = 'Avg Merit/Inst Grant', values = vis_df['Avg_Inst_Grant']),
                dict(range = [0, 1],
                     label = 'Admissions Rate', values = vis_df['Admissions_Rate']),
                dict(range = [0.5, 1],
                     label = 'Graduation Rate', values = vis_df['Graduation_Rate_4yr']),
                dict(range = [vis_df['Net_Price_MidClass'].min(), vis_df['Net_Price_MidClass'].max()],
                     label = 'Your Net Price', values = vis_df['Net_Price_MidClass'])
            ])
        )
    )

    fig_parcoords.update_layout(
        title="2. Finding the Sweet Spot: Balancing Cost, Quality, and Aid"
    )

    return fig_dumbbell, fig_parcoords

# --- 6. MAIN EXECUTION ---

def run_phase_3_pipeline():
    """Executes the full pipeline."""
    
    print("--- Starting Phase 3 ETL Pipeline ---")
    
    # 1. Extraction and Initial Filtering
    data_tuple = load_and_clean_data(IPEDS_BASE_PATH, ESSENTIAL_FILES)
    if data_tuple is None:
        return
    hd, sfa_p1, sfa_p2, ic, adm_sat, adm_rate, gr, mission = data_tuple
    print(f"Data loaded successfully from {len(ESSENTIAL_FILES)} essential files.")

    # 2. Transformation and Metric Calculation
    merged_data = calculate_metrics(hd, sfa_p1, sfa_p2, ic, adm_sat, adm_rate, gr, mission)
    if merged_data.empty:
        return
        
    print(f"Initial Merge Size (after financial data cleanup): {len(merged_data)}")
    print("Metrics calculated: Merit Generosity Index (MGI) and Affordability Score.")
    
    # 3. Final Filtering and Ranking
    final_report = generate_insights(merged_data)
    print(f"Final Report generated. Top 20 colleges selected ({len(final_report)} results).")

    # 4. Loading (Save to CSV)
    output_path = os.path.join(IPEDS_BASE_PATH, OUTPUT_FILE)
    final_report.to_csv(output_path, index=False)
    print(f"Final ranked list saved to: {output_path}")

    # 5. Visualization Generation
    if not final_report.empty:
        fig_d, fig_p = create_visualizations(final_report)
        
        print("\n--- Final Output ---")
        print("Top Ranked Colleges for Middle-Class Merit Aid (Full Report Saved to CSV):")
        
        # Display the output table (you will see this in your console)
        print(final_report[['INSTNM', 'Net_Price_MidClass', 'MGI', 'Composite_Score']].head(10).to_markdown(index=False))
        
        print("\nDisplaying final visualizations (will open in your browser):")
        fig_d.show()
        fig_p.show()
    else:
        print("\nNo colleges met the stringent criteria. Please review filters.")


if __name__ == "__main__":
    run_phase_3_pipeline()