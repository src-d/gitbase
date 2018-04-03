#!/usr/bin/env ruby

require 'benchmark'
require 'linguist'

iterations = (ARGV[0] || 1).to_i

# BenchBlob wraps a FileBlob to keep data loaded and to clean attributes added by language detection.
class BenchBlob < Linguist::FileBlob
  attr_accessor :data
  attr_accessor :fullpath

  def initialize(path, base_path = nil)
    super
    @data = File.read(@fullpath)
  end

  def clean
    @_mime_type = nil
    @detect_encoding = nil
    @lines = nil
  end
end

def get_samples(root)
  samples = Array.new
  Dir.foreach(root) do |file|
    path = File.join(root, file)
    if file == "." or file == ".."
      next
    elsif File.directory?(path)
      get_samples(path).each do |blob|
        samples << blob
      end
    else
      samples << BenchBlob.new(path)
    end
  end
  return samples
end

samples = get_samples('.linguist/samples')
languages = Linguist::Language.all

Benchmark.bmbm do |bm|
  time = bm.report('GetLanguage()_TOTAL ' + iterations.to_s) do
    iterations.times do
      samples.each do |blob|
        Linguist::detect(blob)
        blob.clean
      end
    end
  end
end

Benchmark.bmbm do |bm|
  bm.report('Classify()_TOTAL ' + iterations.to_s) do
    iterations.times do
      samples.each do |blob|
        Linguist::Classifier.classify(Linguist::Samples.cache, blob.data)
        blob.clean
      end
    end
  end
end

Benchmark.bmbm do |bm|
  bm.report('GetLanguagesByModeline()_TOTAL ' + iterations.to_s) do
    iterations.times do
      samples.each do |blob|
        Linguist::Strategy::Modeline.call(blob, languages)
        blob.clean
      end
    end
  end
end

Benchmark.bmbm do |bm|
  bm.report('GetLanguagesByFilename()_TOTAL ' + iterations.to_s) do
    iterations.times do
      samples.each do |blob|
        Linguist::Strategy::Filename.call(blob, languages)
        blob.clean
      end
    end
  end
end

Benchmark.bmbm do |bm|
  bm.report('GetLanguagesByShebang()_TOTAL ' + iterations.to_s) do
    iterations.times do
      samples.each do |blob|
        Linguist::Shebang.call(blob, languages)
        blob.clean
      end
    end
  end
end

Benchmark.bmbm do |bm|
  bm.report('GetLanguagesByExtension()_TOTAL ' + iterations.to_s) do
    iterations.times do
      samples.each do |blob|
        Linguist::Strategy::Extension.call(blob, languages)
        blob.clean
      end
    end
  end
end

Benchmark.bmbm do |bm|
  bm.report('GetLanguagesByContent()_TOTAL ' + iterations.to_s) do
    iterations.times do
      samples.each do |blob|
        Linguist::Heuristics.call(blob, languages)
        blob.clean
      end
    end
  end
end
