package cloud.cholewa.amx.infrastructure.error;

import cloud.cholewa.commons.error.model.ErrorId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

//The codes a caller of the Eaton commands may branch on are the names of these constants. A rename
//compiles and passes every other test of this service, and silently changes what the caller does -
//so the names are written down here a second time
class CustomErrorTest {

    @ParameterizedTest
    @CsvSource({
        "NOT_FOUND_DEVICE",
        "AMBIGUOUS_POINT",
        "NOT_A_DIMMER",
        "FRAME_SERVER_UNAVAILABLE",
        "FRAME_SERVER_TIMEOUT",
        "FRAME_REFUSED"
    })
    void should_keep_the_names_the_callers_rely_on(final String code) {
        assertThat(ErrorId.codeOf(CustomError.valueOf(code))).isEqualTo(code);
    }
}
