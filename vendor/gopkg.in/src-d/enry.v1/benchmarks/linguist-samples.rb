#!/usr/bin/env ruby

require 'benchmark'
require 'linguist'

iterations = (ARGV[0] || 1).to_i

# BenchBlob wraps a FileBlob to keep data loaded and to clean attributes added by language detection.
class BenchBlob < Linguist::FileBlob
  attr_accessor :data

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

samples.each do |blob|
  sample_name = blob.path.gsub(/\s/, '_')
  Benchmark.bmbm do |bm|
    bm.report('GetLanguage()_SAMPLE_' + sample_name + ' ' + iterations.to_s) do
      iterations.times do
        Linguist::detect(blob)
        blob.clean
      end
    end
  end
end

samples.each do |blob|
  sample_name = blob.path.gsub(/\s/, '_')
  Benchmark.bmbm do |bm|
    bm.report('Classify()_SAMPLE_' + sample_name + ' ' + iterations.to_s) do
      iterations.times do
        Linguist::Classifier.classify(Linguist::Samples.cache, blob.data)
        blob.clean
      end
    end
  end
end

samples.each do |blob|
  sample_name = blob.path.gsub(/\s/, '_')
  Benchmark.bmbm do |bm|
    bm.report('GetLanguagesByModeline()_SAMPLE_' + sample_name + ' ' + iterations.to_s) do
      iterations.times do
        Linguist::Strategy::Modeline.call(blob, languages)
        blob.clean
      end
    end
  end
end

samples.each do |blob|
  sample_name = blob.path.gsub(/\s/, '_')
  Benchmark.bmbm do |bm|
    bm.report('GetLanguagesByFilename()_SAMPLE_' + sample_name + ' ' + iterations.to_s) do
    iterations.times do
        Linguist::Strategy::Filename.call(blob, languages)
        blob.clean
      end
    end
  end
end

samples.each do |blob|
  sample_name = blob.path.gsub(/\s/, '_')
  Benchmark.bmbm do |bm|
    bm.report('GetLanguagesByShebang()_SAMPLE_' + sample_name + ' ' + iterations.to_s) do
      iterations.times do
        Linguist::Shebang.call(blob, languages)
        blob.clean
      end
    end
  end
end

samples.each do |blob|
  sample_name = blob.path.gsub(/\s/, '_')
  Benchmark.bmbm do |bm|
    bm.report('GetLanguagesByExtension()_SAMPLE_' + sample_name + ' ' + iterations.to_s) do
      iterations.times do
        Linguist::Strategy::Extension.call(blob, languages)
        blob.clean
      end
    end
  end
end

samples.each do |blob|
  sample_name = blob.path.gsub(/\s/, '_')
  Benchmark.bmbm do |bm|
    bm.report('GetLanguagesByContent()_SAMPLE_' + sample_name + ' ' + iterations.to_s) do
    iterations.times do
        Linguist::Heuristics.call(blob, languages)
        blob.clean
      end
    end
  end
end
