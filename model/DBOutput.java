import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;


/**
 * The DBOutput class is used to connect hadoop output with the SQL database.
 * with the SQL database. The database has description:
 * +----------------+---------------+------+-----+---------+-------+
 * | Field          | Type          | Null | Key | Default | Extra |
 * +----------------+---------------+------+-----+---------+-------+
 * | starting_words | varchar(3000) | YES  |     | NULL    |       |
 * | following_word | varchar(3000) | YES  |     | NULL    |       |
 * | word_count     | int(11)       | YES  |     | NULL    |       |
 * +----------------+---------------+------+-----+---------+-------+
 */
public class DBOutput implements Writable, DBWritable {

    /**
    * String corresponding to field `starting_words` with type `varchar(3000)`
    */
    private String starting_words;

    /**
    * String corresponding to field `following_word` with type `varchar(3000)`
    */
    private String following_word;

    /**
    * Integer corresponding to field word_count` with type `int(11)  `
    */
    private int word_count;

    /**
    * Constructor - set class private variables to given values 
    *
    * @param  starting_words  String, the starting phrase(S)
    * @param  following_word  String, words followed by the starting phrase(F)
    * @param  word_count  Integer, the number of appearance - C(F and S)
    */
    public DBOutput(String starting_words, String following_word, int word_count) {
        this.starting_words = starting_words;
        this.following_word = following_word;
        this.word_count = word_count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(starting_words);
        out.writeChars(following_word);
        out.writeInt(word_count);
    }

    @Override
    public void readFields(DataInput in ) throws IOException {
        starting_words = in.readLine();
        following_word = in.readLine();
        word_count = in.readInt();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, starting_words);
        statement.setString(2, following_word);
        statement.setInt(3, word_count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        starting_words = resultSet.getString(1);
        following_word = resultSet.getString(2);
        word_count = resultSet.getInt(3);
    }
}