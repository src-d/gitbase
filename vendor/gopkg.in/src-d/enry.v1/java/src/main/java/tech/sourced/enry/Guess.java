package tech.sourced.enry;

/**
 * Guess denotes a language detection result of which enry can be
 * completely sure or not.
 */
public class Guess {
    /**
     * The resultant language of the detection.
     */
    public String language;

    /**
     * Indicates whether the enry was completely sure the language is
     * the correct one or it might not be.
     */
    public boolean safe;

    public Guess(String language, boolean safe) {
        this.language = language;
        this.safe = safe;
    }

    @Override
    public String toString() {
        return "Guess{" +
                "language='" + language + '\'' + ", safe=" + safe +
                '}';
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        if (!super.equals(object)) return false;

        Guess guess = (Guess) object;

        if (safe != guess.safe) return false;
        if (language != null ? !language.equals(guess.language) : guess.language != null) return false;

        return true;
    }

    public int hashCode() {
        int result = super.hashCode();
        result = 23 * result + (language != null ? language.hashCode() : 0);
        result = 23 * result + (safe ? 1 : 0);
        return result;
    }
}
