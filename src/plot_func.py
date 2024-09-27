import matplotlib.pyplot as plt

def plot_cashflow(data, x_col, y_cols, title, xlabel='Date', ylabel='Amount (in millions)', figsize=(10, 6)):
    """
    A helper function to plot cashflow data.
    
    Parameters:
    data (pd.DataFrame): The DataFrame containing the data.
    x_col (str): The column name for the x-axis (usually Date).
    y_cols (list of str): List of column names to plot on the y-axis.
    title (str): Title of the plot.
    xlabel (str): Label for the x-axis (default is 'Date').
    ylabel (str): Label for the y-axis (default is 'Amount (in millions)').
    figsize (tuple): Size of the plot (default is (10, 6)).
    """
    plt.figure(figsize=figsize)
    
    # Plot each column in y_cols
    for y_col in y_cols:
        plt.plot(data[x_col], data[y_col], label=y_col)
    
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.show()