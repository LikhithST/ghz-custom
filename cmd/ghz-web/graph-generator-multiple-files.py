import pandas as pd
import matplotlib.pyplot as plt

def plot_combined_latency_graphs(file_paths, colors, legend, output_path='combined_latency_vs_set_id.png'):
    plt.figure(figsize=(10, 6))
    
    # Loop through each file and plot the data
    for i, file_path in enumerate(file_paths):
        xls = pd.ExcelFile(file_path)
        sheet_name = xls.sheet_names[2]  # Access the third sheet by index
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df['latency_milliseconds'] = df['latency_microseconds'] / 1000
        
        # Plotting the data with the specified color
        plt.plot(df['set_id'], df['latency_milliseconds'], marker='o',linestyle='None', color=colors[i], label=f'{legend[i]} - {colors[i]}')
    
    # Adding the titles and labels
    plt.title('Combined Latency Milliseconds vs. Message published')
    plt.xlabel('Message published by Publisher')
    plt.ylabel('Latency (Milliseconds)')
    plt.grid(True)
    
    # Add legend
    plt.legend()
    
    # Save the plot as a PNG file
    plt.savefig(output_path)
    # plt.show()

    print(f'Graph saved to {output_path}')

# Usage example:
file_paths = [
    './test-data/case1.xlsx',
    './test-data/case2.xlsx',
    './test-data/case3.xlsx',
    './test-data/case4.xlsx'
]

colors = ['green','red',  'blue', 'purple']

legend = ['no-congestion', 'nuc6-in-regular-TCP-traffic-congestion', 'L4S-enabled-on-PIs-nuc6-in-regular-TCP-traffic-congestion', 'L4S-enabled-on-PIs-nuc6-in-regular-L4S-traffic-congestion']

plot_combined_latency_graphs(file_paths, colors,legend, output_path='combined_latency_vs_set_id_multi.png')