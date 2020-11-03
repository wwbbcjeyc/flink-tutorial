package com.ztjd.tranfrom;

public class WordCount {
    public String word;

    public int count;

    public WordCount() {
    }

    public WordCount(String word, int count) {
        this.word = word;
        this.count = count;
    }

    //of()方法，用来生成 WordCount 类(Flink源码均使用of()方法形式，省去每次new操作。诸如:Tuple2.of())
    public static WordCount of(String word,int count){
        return new WordCount(word, count);
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }

}
