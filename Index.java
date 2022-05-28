package ru.surf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*; //Использовать "*" нехорошо, из-за этого неясно какие именно классы мы собираемся использовать
import java.util.concurrent.*;
import java.util.stream.Stream;


public class Index {
    TreeMap<String, List<Pointer>> invertedIndex; //Поля принято делать приватными

    ExecutorService pool;

    public Index(ExecutorService pool) {
        this.pool = pool;
        invertedIndex = new TreeMap<>(); //Если обращаемся к своему полю, лучше всегда использовать this для однородности
    }

    public void indexAllTxtInPath(String pathToDir) throws IOException {
        Path of = Path.of(pathToDir); //Название "of" мало о чём говорит, лучше тогда уж "path"

        BlockingQueue<Path> files = new ArrayBlockingQueue<>(2);

        try (Stream<Path> stream = Files.list(of)) {
            stream.forEach(files::add); //У очереди выставлен размер 2, это так не будет работать, если файлов больше 2х из-за размера очереди
        }

        pool.submit(new IndexTask(files)); //Почему именно 3 раза? Почему нельзя было в цикле создавать?
        pool.submit(new IndexTask(files)); //Если мы не ждём никакого результата выполнения, то можно использовать execute вместо submit
        pool.submit(new IndexTask(files)); //Вообще у нас нет никакого способа узнать - завершились ли таски
    }

    public TreeMap<String, List<Pointer>> getInvertedIndex() {
        return invertedIndex;
    }

    public List<Pointer> GetRelevantDocuments(String term) { //Названия методов с маленькой буквы
        return invertedIndex.get(term);
    }

    public Optional<Pointer> getMostRelevantDocument(String term) {
        return invertedIndex.get(term).stream().max(Comparator.comparing(o -> o.count));
    }

    static class Pointer {
        private Integer count;
        private String filePath;

        public Pointer(Integer count, String filePath) {
            this.count = count;
            this.filePath = filePath;
        }

        @Override
        public String toString() {
            return "{" + "count=" + count + ", filePath='" + filePath + '\'' + '}';
        }
    }

    class IndexTask implements Runnable {

        private final BlockingQueue<Path> queue;

        public IndexTask(BlockingQueue<Path> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                Path take = queue.take(); //Название переменной take мало о чём говорит, лучше просто path
                List<String> strings = Files.readAllLines(take);

                strings.stream().flatMap(str -> Stream.of(str.split(" "))).forEach(word -> invertedIndex.compute(word, (k, v) -> {
                    if (v == null) return List.of(new Pointer(1, take.toString()));
                    else {
                        ArrayList<Pointer> pointers = new ArrayList<>();

                        if (v.stream().noneMatch(pointer -> pointer.filePath.equals(take.toString()))) {
                            pointers.add(new Pointer(1, take.toString()));
                        }
                        //Эти два блока можно объединить в if else
                        v.forEach(pointer -> {
                            if (pointer.filePath.equals(take.toString())) {
                                pointer.count = pointer.count + 1;
                            }
                        });

                        pointers.addAll(v);

                        return pointers;
                    }

                }));

            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(); //Не надо выкидывать пустой RuntimeException, передай в него ошибку, которая пришла: RuntimeException(e)
            }
        }
    }
}
