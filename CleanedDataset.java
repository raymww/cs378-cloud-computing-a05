import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CleanedDataset {
    public static void main(String[] args) {
        // Check if the input file path is provided
        if (args.length < 1) {
            System.err.println("Please provide the input CSV file path as an argument.");
            System.exit(1);
        }

        String inputFilePath = args[0];

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
             BufferedWriter writer = new BufferedWriter(new FileWriter("cleaned_dataset_large.csv", false))) {
            String line;

            // Process each line from the input CSV file
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(","); // Split by comma

                if (fields.length < 16) { // Check if enough fields are present
                    System.err.println("Skipping malformed line: " + line);
                    continue; // Skip this line if not enough fields
                }

                try {
                    String driverID = fields[1].trim();
                    double distance = Double.parseDouble(fields[5]);
                    double fare_amount = Double.parseDouble(fields[11]);
                    double tolls_amount = Double.parseDouble(fields[15]);
                    double time = Double.parseDouble(fields[4]);

                    // Filter criteria
                    if (tolls_amount < 3 || fare_amount < 3 || fare_amount > 200 ||
                        distance < 1 || distance > 50 || time < 120 || time > 3600) {
                        throw new Exception("Filtered out based on criteria.");
                    }

                    // Write the line to the output file if it passes the filters
                    writer.write(line);
                    writer.newLine(); // Use newLine() for better compatibility
                } catch (NumberFormatException e) {
                    System.err.println("Skipping line due to number format issue: " + line);
                    continue; // Skip the line if a number format exception occurs
                } catch (Exception e) {
                    // Handle other exceptions (e.g., filtering criteria)
                    System.err.println("Skipping line due to filtering: " + line);
                    continue;
                }
            }
        } catch (IOException e) {
            System.err.println("Error processing the file: " + e.getMessage());
        }
    }
}