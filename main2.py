import numpy as np
import pandas as pd

# Load data in batches from CSV
def load_data_in_batches(file_path, chunksize):
    return pd.read_csv(file_path, chunksize=chunksize)

# Normalize the data
def normalize_data(batch):
    return (batch - batch.mean()) / batch.std()

# Gradient Descent implementation
def gradient_descent(file_path, learning_rate=0.001, num_iterations=100, chunksize=1000):
    # Initialize parameters
    m1, m2, m3, m4, b = 0.0, 0.0, 0.0, 0.0, 0.0

    # Load data in batches
    batches = load_data_in_batches(file_path, chunksize)

    # Calculate total samples
    total_samples = sum(len(batch) for batch in pd.read_csv(file_path, chunksize=chunksize))

    # Iterate for the number of iterations
    for iteration in range(num_iterations):
        # Reset gradients
        m1_gradient, m2_gradient, m3_gradient, m4_gradient, b_gradient = 0, 0, 0, 0, 0
        
        # Process each batch
        for batch in load_data_in_batches(file_path, chunksize):
            # Extract and normalize X and y
            X1_batch = pd.to_numeric(batch.iloc[:, 4]).values  # trip_time
            X2_batch = pd.to_numeric(batch.iloc[:, 5]).values  # trip_distance
            X3_batch = pd.to_numeric(batch.iloc[:, 11]).values  # fare_amount
            X4_batch = pd.to_numeric(batch.iloc[:, 15]).values  # tolls_amount
            y_batch = pd.to_numeric(batch.iloc[:, 16]).values  # total_amount

            # Normalize features
            X1_batch = normalize_data(X1_batch)
            X2_batch = normalize_data(X2_batch)
            X3_batch = normalize_data(X3_batch)
            X4_batch = normalize_data(X4_batch)

            # Compute predictions
            predictions = (m1 * X1_batch + m2 * X2_batch + m3 * X3_batch + m4 * X4_batch + b)

            # Calculate gradients
            m1_gradient += -2 / total_samples * np.sum(X1_batch * (y_batch - predictions))
            m2_gradient += -2 / total_samples * np.sum(X2_batch * (y_batch - predictions))
            m3_gradient += -2 / total_samples * np.sum(X3_batch * (y_batch - predictions))
            m4_gradient += -2 / total_samples * np.sum(X4_batch * (y_batch - predictions))
            b_gradient += -2 / total_samples * np.sum(y_batch - predictions)

        # Optionally clip gradients to avoid overflow
        clip_value = 1e5  # Adjust as necessary
        m1_gradient = np.clip(m1_gradient, -clip_value, clip_value)
        m2_gradient = np.clip(m2_gradient, -clip_value, clip_value)
        m3_gradient = np.clip(m3_gradient, -clip_value, clip_value)
        m4_gradient = np.clip(m4_gradient, -clip_value, clip_value)
        b_gradient = np.clip(b_gradient, -clip_value, clip_value)

        # Update parameters
        m1 -= learning_rate * m1_gradient
        m2 -= learning_rate * m2_gradient
        m3 -= learning_rate * m3_gradient
        m4 -= learning_rate * m4_gradient
        b -= learning_rate * b_gradient

        print(f"Iteration {iteration + 1}: m1 = {m1}, m2 = {m2}, m3 = {m3}, m4 = {m4}, b = {b}")

    return m1, m2, m3, m4, b

# Run gradient descent
optimal_m1, optimal_m2, optimal_m3, optimal_m4, optimal_b = gradient_descent('cleaned_dataset.csv')
print(f"Optimal parameters: m1 = {optimal_m1}, m2 = {optimal_m2}, m3 = {optimal_m3}, m4 = {optimal_m4}, b = {optimal_b}")