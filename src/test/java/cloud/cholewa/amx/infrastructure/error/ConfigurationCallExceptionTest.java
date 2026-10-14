package cloud.cholewa.amx.infrastructure.error;

import cloud.cholewa.commons.error.model.ErrorMessage;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

//the line of the log is the only place a failed lookup is told: the status, the cause as this
//service names it and - after "downstream:" - what database-service itself said
class ConfigurationCallExceptionTest {

    @Test
    void should_describe_the_status_and_the_cause() {
        final ConfigurationCallException exception = new ConfigurationCallException(
            HttpStatus.NOT_FOUND, Set.of(ErrorMessage.builder().message("m").details("d").build())
        );

        assertThat(exception.describe()).isEqualTo("lookup failed (404): d");
    }

    @Test
    void should_describe_the_downstream_messages_after_the_cause() {
        final ConfigurationCallException exception = new ConfigurationCallException(
            HttpStatus.BAD_GATEWAY,
            Set.of(ErrorMessage.builder().message("m").details("database-service answered 500").build()),
            Set.of(ErrorMessage.builder().message("Internal error").details("bad SQL grammar").build())
        );

        assertThat(exception.describe())
            .isEqualTo("lookup failed (502): database-service answered 500 - downstream: bad SQL grammar");
    }

    @Test
    void should_describe_an_exception_without_a_status_or_messages() {
        assertThat(new ConfigurationCallException(null, null, null).describe()).isEqualTo("lookup failed (no status): ");
    }

    @Test
    void should_fall_back_to_the_message_when_an_error_has_no_details() {
        final ConfigurationCallException exception = new ConfigurationCallException(
            HttpStatus.BAD_GATEWAY, Set.of(ErrorMessage.builder().message("boom").build())
        );

        assertThat(exception.describe()).isEqualTo("lookup failed (502): boom");
    }
}
