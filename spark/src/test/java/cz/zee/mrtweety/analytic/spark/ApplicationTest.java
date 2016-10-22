package cz.zee.mrtweety.analytic.spark;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import twitter4j.*;
import twitter4j.conf.Configuration;

/**
 * @author Jakub Horak
 */
public class ApplicationTest {

    private Application underTest;

    @Before
    public void setUp() throws Exception {
        underTest = new Application();
    }

    @Ignore
    @Test
    public void getOauthConfiguration() throws Exception {

        Configuration oauthConfiguration = underTest.getOauthConfiguration();

        TwitterStream twitterStream = new TwitterStreamFactory(oauthConfiguration).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);

        FilterQuery filterQuery = new FilterQuery("europe", "eu");
        twitterStream.filter(filterQuery);
        Thread.sleep(10000);
    }

}