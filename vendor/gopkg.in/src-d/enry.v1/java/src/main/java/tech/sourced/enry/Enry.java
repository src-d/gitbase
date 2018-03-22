package tech.sourced.enry;

import tech.sourced.enry.nativelib.*;

import static tech.sourced.enry.GoUtils.*;

public class Enry {
    public static final Guess unknownLanguage = new Guess("", false);

    private static final EnryLibrary nativeLib = EnryLibrary.INSTANCE;

    /**
     * Returns whether the given language is auxiliary or not.
     *
     * @param language name of the language, e.g. PHP, HTML, ...
     * @return if it's an auxiliary language
     */
    public static synchronized boolean isAuxiliaryLanguage(String language) {
        return toJavaBool(nativeLib.IsAuxiliaryLanguage(toGoString(language)));
    }

    /**
     * Returns the language of the given file based on the filename and its
     * contents.
     *
     * @param filename name of the file with the extension
     * @param content  array of bytes with the contents of the file (the code)
     * @return the guessed language
     */
    public static synchronized String getLanguage(String filename, byte[] content) {
        return toJavaString(nativeLib.GetLanguage(
                toGoString(filename),
                toGoByteSlice(content)
        ));
    }

    /**
     * Returns detected language by its content.
     * If there are more than one possible language, it returns the first
     * language in alphabetical order and safe to false.
     *
     * @param filename name of the file with the extension
     * @param content  of the file
     * @return guessed result
     */
    public static synchronized Guess getLanguageByContent(String filename, byte[] content) {
        GetLanguageByContent_return.ByValue res = nativeLib.GetLanguageByContent(
                toGoString(filename),
                toGoByteSlice(content)
        );
        return new Guess(toJavaString(res.r0), toJavaBool(res.r1));
    }

    /**
     * Returns detected language by its emacs modeline.
     * If there are more than one possible language, it returns the first
     * language in alphabetical order and safe to false.
     *
     * @param content of the file
     * @return guessed result
     */
    public static synchronized Guess getLanguageByEmacsModeline(byte[] content) {
        GetLanguageByEmacsModeline_return.ByValue res = nativeLib.GetLanguageByEmacsModeline(toGoByteSlice(content));
        return new Guess(toJavaString(res.r0), toJavaBool(res.r1));
    }

    /**
     * Returns detected language by the extension of the filename.
     * If there are more than one possible languages, it returns
     * the first language in alphabetical order and safe to false.
     *
     * @param filename of the file
     * @return guessed result
     */
    public static synchronized Guess getLanguageByExtension(String filename) {
        GetLanguageByExtension_return.ByValue res = nativeLib.GetLanguageByExtension(toGoString(filename));
        return new Guess(toJavaString(res.r0), toJavaBool(res.r1));
    }

    /**
     * Returns detected language by its shebang.
     * If there are more than one possible language, it returns the first
     * language in alphabetical order and safe to false.
     *
     * @param content of the file
     * @return guessed result
     */
    public static synchronized Guess getLanguageByShebang(byte[] content) {
        GetLanguageByShebang_return.ByValue res = nativeLib.GetLanguageByShebang(toGoByteSlice(content));
        return new Guess(toJavaString(res.r0), toJavaBool(res.r1));
    }

    /**
     * Returns detected language by its filename.
     * If there are more than one possible language, it returns the first
     * language in alphabetical order and safe to false.
     *
     * @param filename of the file
     * @return guessed result
     */
    public static synchronized Guess getLanguageByFilename(String filename) {
        GetLanguageByFilename_return.ByValue res = nativeLib.GetLanguageByFilename(toGoString(filename));
        return new Guess(toJavaString(res.r0), toJavaBool(res.r1));
    }

    /**
     * Returns detected language by its modeline.
     * If there are more than one possible language, it returns the first
     * language in alphabetical order and safe to false.
     *
     * @param content of the file
     * @return guessed result
     */
    public static synchronized Guess getLanguageByModeline(byte[] content) {
        GetLanguageByModeline_return.ByValue res = nativeLib.GetLanguageByModeline(toGoByteSlice(content));
        return new Guess(toJavaString(res.r0), toJavaBool(res.r1));
    }

    /**
     * Returns detected language by its vim modeline.
     * If there are more than one possible language, it returns the first
     * language in alphabetical order and safe to false.
     *
     * @param content of the file
     * @return guessed result
     */
    public static synchronized Guess getLanguageByVimModeline(byte[] content) {
        GetLanguageByVimModeline_return.ByValue res = nativeLib.GetLanguageByVimModeline(toGoByteSlice(content));
        return new Guess(toJavaString(res.r0), toJavaBool(res.r1));
    }

    /**
     * Returns all the possible extensions for a file in the given language.
     *
     * @param language to get extensions from
     * @return extensions
     */
    public static synchronized String[] getLanguageExtensions(String language) {
        GoSlice result = new GoSlice();
        nativeLib.GetLanguageExtensions(toGoString(language), result);
        return toJavaStringArray(result);
    }

    /**
     * Returns all possible languages for the given file.
     *
     * @param filename of the file
     * @param content  of the file
     * @return all possible languages
     */
    public static synchronized String[] getLanguages(String filename, byte[] content) {
        GoSlice result = new GoSlice();
        nativeLib.GetLanguages(toGoString(filename), toGoByteSlice(content), result);
        return toJavaStringArray(result);
    }

    /**
     * Returns the mime type of the file.
     *
     * @param path     of the file
     * @param language of the file
     * @return mime type
     */
    public static synchronized String getMimeType(String path, String language) {
        return toJavaString(nativeLib.GetMimeType(toGoString(path), toGoString(language)));
    }

    /**
     * Reports whether the given file content is binary or not.
     *
     * @param content of the file
     * @return whether it's binary or not
     */
    public static synchronized boolean isBinary(byte[] content) {
        return toJavaBool(nativeLib.IsBinary(toGoByteSlice(content)));
    }

    /**
     * Reports whether the given file or directory is a config file or directory.
     *
     * @param path of the file or directory
     * @return whether it's config or not
     */
    public static synchronized boolean isConfiguration(String path) {
        return toJavaBool(nativeLib.IsConfiguration(toGoString(path)));
    }

    /**
     * Reports whether the given file or directory it's documentation.
     *
     * @param path of the file or directory. It must not contain its parents and
     *             if it's a directory it must end in a slash e.g. "docs/" or
     *             "foo.json".
     * @return whether it's docs or not
     */
    public static synchronized boolean isDocumentation(String path) {
        return toJavaBool(nativeLib.IsDocumentation(toGoString(path)));
    }

    /**
     * Reports whether the given file is a dotfile.
     *
     * @param path of the file
     * @return whether it's a dotfile or not
     */
    public static synchronized boolean isDotFile(String path) {
        return toJavaBool(nativeLib.IsDotFile(toGoString(path)));
    }

    /**
     * Reports whether the given path is an image or not.
     *
     * @param path of the file
     * @return whether it's an image or not
     */
    public static synchronized boolean isImage(String path) {
        return toJavaBool(nativeLib.IsImage(toGoString(path)));
    }

    /**
     * Reports whether the given path is a vendor path or not.
     *
     * @param path of the file or directory
     * @return whether it's vendor or not
     */
    public static synchronized boolean isVendor(String path) {
        return toJavaBool(nativeLib.IsVendor(toGoString(path)));
    }

}
