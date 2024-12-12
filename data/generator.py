import random
import os

# File generation settings
OUTPUT_DIR = "input_files"  # Directory to store the generated files
WORD_LIST = ["hello", "world", "beam", "performance", "evaluation", "pipeline", "flume", "java"]
SMALL_FILE_WORDS = 1_000
MEDIUM_FILE_WORDS = 10_000
LARGE_FILE_WORDS = 100_000

def generate_text_file(file_path, total_words, repetitive=False):
    """
    Generates a text file with the specified number of words.
    :param file_path: Path to save the generated file.
    :param total_words: Total number of words to include.
    :param repetitive: Whether to use a single repeated sentence or random words.
    """
    with open(file_path, "w") as f:
        if repetitive:
            sentence = " ".join(random.choices(WORD_LIST, k=10)) + "\n"
            for _ in range(total_words // 10):
                f.write(sentence)
        else:
            for _ in range(total_words):
                word = random.choice(WORD_LIST)
                f.write(word + " ")
                if random.random() < 0.1:  # Add a new line roughly every 10 words
                    f.write("\n")
    print(f"File generated: {file_path} ({total_words} words)")

def main():
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Generate small, medium, and large input files
    small_file = os.path.join(OUTPUT_DIR, "small_input.txt")
    medium_file = os.path.join(OUTPUT_DIR, "medium_input.txt")
    large_file = os.path.join(OUTPUT_DIR, "large_input.txt")

    print("Generating input files...")
    generate_text_file(small_file, SMALL_FILE_WORDS)
    generate_text_file(medium_file, MEDIUM_FILE_WORDS)
    generate_text_file(large_file, LARGE_FILE_WORDS)

    # Optional: Generate a repetitive version of the large file
    repetitive_large_file = os.path.join(OUTPUT_DIR, "large_repetitive_input.txt")
    generate_text_file(repetitive_large_file, LARGE_FILE_WORDS, repetitive=True)
    print("Input file generation complete!")

if __name__ == "__main__":
    main()
