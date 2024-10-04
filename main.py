import numpy as np
import pandas as pd

# Load data in batches from CSV
def load_data_in_batches(file_path, chunksize):
    return pd.read_csv(file_path, chunksize=chunksize)

# Gradient Descent implementation
def gradient_descent(file_path, learning_rate=0.001, num_iterations=100, chunksize=1000):
    # Initialize parameters
    m = 0.0  # slope
    b = 0.0  # intercept
    n = 0  # total number of samples

    # Load data in batches
    batches = load_data_in_batches(file_path, chunksize)

    # Iterate for the number of iterations
    for iteration in range(num_iterations):
        m_gradient = 0
        b_gradient = 0
        total_samples = 0

        # Process each batch
        for batch in batches:
            # Extract X and y (Column 6 is index 5, and Column 17 is index 16)
            X_batch = pd.to_numeric(batch.iloc[:, 5]).values  # Column 5 for distance
            y_batch = pd.to_numeric(batch.iloc[:, 11]).values  # Column 16 for total_amount
            

            # Update the number of samples
            total_samples += len(X_batch)

            m_gradient += np.sum(-X_batch * (y_batch - (m * X_batch + b)))
            b_gradient += np.sum(-1 * (y_batch - (m * X_batch + b)))

        m_gradient *= 2 / total_samples
        b_gradient *= 2 / total_samples

        m -= learning_rate * m_gradient
        b -= learning_rate * b_gradient

        print(f"Iteration {iteration + 1}: m = {m}, b = {b}")

        batches = load_data_in_batches(file_path, chunksize)

    return m, b

optimal_m, optimal_b = gradient_descent('cleaned_dataset.csv')
print(f"Optimal parameters: m = {optimal_m}, b = {optimal_b}")