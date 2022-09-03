package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import java.util.Map;
import java.util.Objects;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    private final MongoCollection<Session> sessionsCollection;
    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        try {
            usersCollection.insertOne(user);
            return true;
        } catch (MongoWriteException ex) {
            log.error(ex.getMessage(),ex.getCause());
            throw new IncorrectDaoOperation(ex.getMessage(), ex.getCause());
        }
    }

        /**
         * Creates session using userId and jwt token.
         *
         * @param userId - user string identifier
         * @param jwt    - jwt string token
         * @return true if successful
         */
        public boolean createUserSession(String userId, String jwt) {
                Session session = new Session();
                session.setUserId(userId);
                session.setJwt(jwt);
            if(sessionsCollection.find(new Document("user_id", userId)).iterator().hasNext()) {
                sessionsCollection.updateOne(new Document("user_id", userId), new Document("$set",new Document("jwt", jwt)));
                return true;
            }
            try{
                sessionsCollection.insertOne(session);
                return true;
            }
            catch (IncorrectDaoOperation ex) {
                throw new IncorrectDaoOperation(userId);
            }
        }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        User user = null;
        Bson findQuery = new Document("email", email);
        user = usersCollection.find(findQuery).iterator().tryNext();
        return user;
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        return sessionsCollection.find(new Document("user_id", userId)).iterator().tryNext();
    }

    public boolean deleteUserSessions(String userId) {
        sessionsCollection.findOneAndDelete(new Document("user_id", userId));
        return true;
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        try {
            usersCollection.findOneAndDelete(new Document("email", email));
            if(sessionsCollection.find(new Document("user_id", email)).iterator().hasNext()){
            sessionsCollection.findOneAndDelete(new Document("user_id", email));}
            return true;
        } catch (IncorrectDaoOperation ex) {
            throw ex;
        }
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        if (Objects.isNull(userPreferences)){
            throw new IncorrectDaoOperation("preferences is null");
        }
        try {
            usersCollection.updateOne(
                    new Document("email", email),
                    new Document("$set", new Document("preferences", userPreferences)));
            return true;
        } catch (IncorrectDaoOperation ex) {
            log.error(ex.getMessage());
            throw ex;
        }

    }
}
