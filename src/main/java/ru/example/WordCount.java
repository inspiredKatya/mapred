package ru.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Пример Map-Reduce задачи для подсчета слов в тексте.
 * Запуск:
 * hadoop jar map-reduce-1.0.jar ru.example.WordCount input output
 */
public class WordCount {

    /**
     * Класс мэппера, который принимает на вход текст входного файла,
     * разбивает его на слова и возвращает пары (слово, 1).
     * "1" в дальнейшем будет количеством слов.
     */
    private static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
            for(String foundWord: words){
                word.set(foundWord);
                context.write(word, one);
            }
        }
    }

    /**
     * Редьюсер (он же комбайнер).
     * Принимает на вход сгруппированные по ключу (слову) коллекцию их частот (кол-во встреченных слов).
     * Суммирует эти числа и возвращает одну пару (слово, количество).
     */
    private static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    static class LetterPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text text, IntWritable intWritable, int numPartitions) {

            if (text.getLength() == 0 || numPartitions == 1) {
                return 0;
            }
            int firstChar = Character.toUpperCase(text.charAt(0));
            int englishLettersPerPartition = ('Z' - 'A' + 1) / (numPartitions - 1) + 1;
            int russianLettersPerPartition = ('Я' - 'А' + 1) / (numPartitions - 1) + 1;
            if (firstChar >= 'A' && firstChar <= 'Z') {
                return (firstChar - 'A') / englishLettersPerPartition + 1;
            } else if (firstChar >= 'А' && firstChar <= 'Я') {
                return (firstChar - 'А') / russianLettersPerPartition + 1;
            } else {
                return 0;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Создаем объект конфигурации
        Configuration conf = new Configuration();
        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(conf, "word count");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(WordCount.class);
        // Указываем класс Маппера
        job.setMapperClass(TokenizerMapper.class);
        // Класс Комбайнера
        job.setCombinerClass(IntSumReducer.class);
        // Редьюсера - одна и та же реализация для слияния изменений
        job.setReducerClass(IntSumReducer.class);
        // Установка класса для партиционирования
        job.setPartitionerClass(LetterPartitioner.class);
        // Тип ключа на выходе
        job.setOutputKeyClass(Text.class);
        // Тип значения на выходе
        job.setOutputValueClass(IntWritable.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //кол-во редьюсеров = кол-во партиций
        job.setNumReduceTasks(2);

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        System.exit(success ? 0 : 1);
    }
}
