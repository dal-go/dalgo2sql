package dalgo2sql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"sync"

	"modernc.org/sqlite"
)

const maxDecimalDigits = 38

var (
	decimalRegistration    sync.Once
	decimalRegistrationErr error
	decimalPattern         = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$`)
)

// RegisterSQLiteDecimalFunctions registers exact decimal functions on modernc's
// process-wide SQLite driver. Call it before opening the first SQLite connection.
// Values are accepted as decimal strings, byte strings, or exact integer values;
// SQLite REAL values are rejected because their original decimal value is lost.
// Results are decimal strings so SQLite stores them without a binary-float round trip.
//
// Scalar functions propagate NULL. decimal_cmp returns -1, 0, or 1. Division,
// rounding, and average use round-half-to-even. Scales must be between 0 and 38.
// decimal_sum and decimal_avg ignore NULL inputs and return NULL for no non-NULL rows.
func RegisterSQLiteDecimalFunctions() error {
	decimalRegistration.Do(func() {
		decimalRegistrationErr = registerSQLiteDecimalFunctions()
	})
	return decimalRegistrationErr
}

func registerSQLiteDecimalFunctions() error {
	scalars := []struct {
		name string
		args int32
		fn   func([]driver.Value) (driver.Value, error)
	}{
		{"decimal_add", 2, decimalBinary(func(a, b *big.Rat) *big.Rat { return new(big.Rat).Add(a, b) })},
		{"decimal_sub", 2, decimalBinary(func(a, b *big.Rat) *big.Rat { return new(big.Rat).Sub(a, b) })},
		{"decimal_mul", 2, decimalBinary(func(a, b *big.Rat) *big.Rat { return new(big.Rat).Mul(a, b) })},
		{"decimal_cmp", 2, decimalCompare},
		{"decimal_div", 3, decimalDivide},
		{"decimal_round", 2, decimalRound},
	}
	for _, item := range scalars {
		name, fn := item.name, item.fn
		if err := sqlite.RegisterDeterministicScalarFunction(name, item.args, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			for _, arg := range args {
				if arg == nil {
					return nil, nil
				}
			}
			return fn(args)
		}); err != nil {
			return fmt.Errorf("register SQLite %s: %w", name, err)
		}
	}
	for _, item := range []struct {
		name string
		args int32
		avg  bool
	}{{"decimal_sum", 1, false}, {"decimal_avg", 2, true}} {
		name, avg := item.name, item.avg
		if err := sqlite.RegisterFunction(name, &sqlite.FunctionImpl{
			NArgs: item.args,
			MakeAggregate: func(sqlite.FunctionContext) (sqlite.AggregateFunction, error) {
				return &decimalAggregate{average: avg}, nil
			},
		}); err != nil {
			return fmt.Errorf("register SQLite %s: %w", name, err)
		}
	}
	if err := sqlite.RegisterCollationUtf8("DECIMAL", compareDecimalText); err != nil {
		return fmt.Errorf("register SQLite DECIMAL collation: %w", err)
	}
	return nil
}

// compareDecimalText provides a total order even if a column contains malformed
// values: valid decimal text sorts numerically before invalid text; invalid text
// sorts bytewise. Applications should constrain DECIMAL_TEXT columns to valid
// decimal strings when they rely on numeric order.
func compareDecimalText(left, right string) int {
	a, errA := parseExactDecimal(left)
	b, errB := parseExactDecimal(right)
	switch {
	case errA == nil && errB == nil:
		return a.rat().Cmp(b.rat())
	case errA == nil:
		return -1
	case errB == nil:
		return 1
	default:
		return strings.Compare(left, right)
	}
}

type exactDecimal struct {
	coefficient big.Int
	scale       int
}

func parseExactDecimal(value driver.Value) (exactDecimal, error) {
	var text string
	switch v := value.(type) {
	case string:
		text = v
	case []byte:
		text = string(v)
	case int64:
		text = fmt.Sprintf("%d", v)
	case int:
		text = fmt.Sprintf("%d", v)
	default:
		return exactDecimal{}, fmt.Errorf("decimal input must be text or an exact integer, got %T", value)
	}
	if len(text) == 0 || len(text) > 80 || !decimalPattern.MatchString(text) {
		return exactDecimal{}, fmt.Errorf("invalid decimal text %q", text)
	}
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(strings.TrimPrefix(text, "+"), "-")
	parts := strings.SplitN(text, ".", 2)
	whole, fraction := parts[0], ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		digits = "0"
	}
	if len(digits) > maxDecimalDigits || len(fraction) > maxDecimalDigits {
		return exactDecimal{}, fmt.Errorf("decimal exceeds %d digits or scale", maxDecimalDigits)
	}
	var coeff big.Int
	if _, ok := coeff.SetString(digits, 10); !ok {
		return exactDecimal{}, fmt.Errorf("invalid decimal text %q", text)
	}
	if negative && coeff.Sign() != 0 {
		coeff.Neg(&coeff)
	}
	return exactDecimal{coefficient: coeff, scale: len(fraction)}, nil
}

func (d exactDecimal) text() string {
	coeff := new(big.Int).Set(&d.coefficient)
	negative := coeff.Sign() < 0
	coeff.Abs(coeff)
	digits := coeff.String()
	if d.scale > 0 {
		if len(digits) <= d.scale {
			digits = strings.Repeat("0", d.scale-len(digits)+1) + digits
		}
		at := len(digits) - d.scale
		digits = digits[:at] + "." + digits[at:]
		digits = strings.TrimRight(strings.TrimRight(digits, "0"), ".")
	}
	if digits == "" || digits == "0" {
		return "0"
	}
	if negative {
		return "-" + digits
	}
	return digits
}

func (d exactDecimal) rat() *big.Rat {
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.scale)), nil)
	return new(big.Rat).SetFrac(new(big.Int).Set(&d.coefficient), denominator)
}

func decimalFromRat(value *big.Rat) (exactDecimal, error) {
	denom := new(big.Int).Set(value.Denom())
	two, five := big.NewInt(2), big.NewInt(5)
	twos, fives := 0, 0
	for new(big.Int).Mod(denom, two).Sign() == 0 {
		denom.Div(denom, two)
		twos++
	}
	for new(big.Int).Mod(denom, five).Sign() == 0 {
		denom.Div(denom, five)
		fives++
	}
	if denom.Cmp(big.NewInt(1)) != 0 {
		return exactDecimal{}, errors.New("decimal result is non-terminating; use decimal_div with a scale")
	}
	scale := twos
	if fives > scale {
		scale = fives
	}
	if scale > maxDecimalDigits {
		return exactDecimal{}, fmt.Errorf("decimal result scale exceeds %d", maxDecimalDigits)
	}
	coeff := new(big.Int).Set(value.Num())
	if twos < scale {
		coeff.Mul(coeff, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(scale-twos)), nil))
	}
	if fives < scale {
		coeff.Mul(coeff, new(big.Int).Exp(big.NewInt(5), big.NewInt(int64(scale-fives)), nil))
	}
	if len(new(big.Int).Abs(coeff).String()) > maxDecimalDigits {
		return exactDecimal{}, fmt.Errorf("decimal result exceeds %d digits", maxDecimalDigits)
	}
	return exactDecimal{coefficient: *coeff, scale: scale}, nil
}

func decimalBinary(op func(a, b *big.Rat) *big.Rat) func([]driver.Value) (driver.Value, error) {
	return func(args []driver.Value) (driver.Value, error) {
		a, err := parseExactDecimal(args[0])
		if err != nil {
			return nil, err
		}
		b, err := parseExactDecimal(args[1])
		if err != nil {
			return nil, err
		}
		result, err := decimalFromRat(op(a.rat(), b.rat()))
		if err != nil {
			return nil, err
		}
		return result.text(), nil
	}
}

func decimalCompare(args []driver.Value) (driver.Value, error) {
	a, err := parseExactDecimal(args[0])
	if err != nil {
		return nil, err
	}
	b, err := parseExactDecimal(args[1])
	if err != nil {
		return nil, err
	}
	return int64(a.rat().Cmp(b.rat())), nil
}

func decimalScale(value driver.Value) (int, error) {
	v, ok := value.(int64)
	if !ok || v < 0 || v > maxDecimalDigits {
		return 0, fmt.Errorf("decimal scale must be an integer from 0 to %d", maxDecimalDigits)
	}
	return int(v), nil
}

func roundQuotientEven(numerator, denominator *big.Int) *big.Int {
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, denominator, remainder)
	comparison := new(big.Int).Lsh(new(big.Int).Abs(remainder), 1).Cmp(new(big.Int).Abs(denominator))
	if comparison > 0 || (comparison == 0 && quotient.Bit(0) == 1) {
		if numerator.Sign()*denominator.Sign() < 0 {
			quotient.Sub(quotient, big.NewInt(1))
		} else {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	return quotient
}

func rounded(value *big.Rat, scale int) (exactDecimal, error) {
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	numerator := new(big.Int).Mul(value.Num(), factor)
	coeff := roundQuotientEven(numerator, value.Denom())
	if len(new(big.Int).Abs(coeff).String()) > maxDecimalDigits {
		return exactDecimal{}, fmt.Errorf("decimal result exceeds %d digits", maxDecimalDigits)
	}
	return exactDecimal{coefficient: *coeff, scale: scale}, nil
}

func decimalDivide(args []driver.Value) (driver.Value, error) {
	a, err := parseExactDecimal(args[0])
	if err != nil {
		return nil, err
	}
	b, err := parseExactDecimal(args[1])
	if err != nil {
		return nil, err
	}
	if b.coefficient.Sign() == 0 {
		return nil, errors.New("decimal division by zero")
	}
	scale, err := decimalScale(args[2])
	if err != nil {
		return nil, err
	}
	result, err := rounded(new(big.Rat).Quo(a.rat(), b.rat()), scale)
	if err != nil {
		return nil, err
	}
	return result.text(), nil
}

func decimalRound(args []driver.Value) (driver.Value, error) {
	value, err := parseExactDecimal(args[0])
	if err != nil {
		return nil, err
	}
	scale, err := decimalScale(args[1])
	if err != nil {
		return nil, err
	}
	result, err := rounded(value.rat(), scale)
	if err != nil {
		return nil, err
	}
	return result.text(), nil
}

type decimalAggregate struct {
	average  bool
	seen     bool
	sum      big.Rat
	count    int64
	scale    int
	scaleSet bool
	err      error
}

func (a *decimalAggregate) Step(_ *sqlite.FunctionContext, args []driver.Value) error {
	if a.err != nil {
		return a.err
	}
	if args[0] == nil {
		return nil
	}
	value, err := parseExactDecimal(args[0])
	if err != nil {
		a.err = err
		return err
	}
	a.sum.Add(&a.sum, value.rat())
	a.seen = true
	if a.count == int64(^uint64(0)>>1) {
		a.err = errors.New("decimal aggregate row count overflow")
		return a.err
	}
	a.count++
	if a.average {
		scale, err := decimalScale(args[1])
		if err != nil {
			a.err = err
			return err
		}
		if a.scaleSet && scale != a.scale {
			a.err = errors.New("decimal_avg scale must be constant within a group")
			return a.err
		}
		a.scale, a.scaleSet = scale, true
	}
	return nil
}

func (a *decimalAggregate) WindowInverse(_ *sqlite.FunctionContext, _ []driver.Value) error {
	return errors.New("decimal aggregates do not support window frames")
}

func (a *decimalAggregate) WindowValue(_ *sqlite.FunctionContext) (driver.Value, error) {
	return a.result()
}

func (a *decimalAggregate) Final(_ *sqlite.FunctionContext) {}

func (a *decimalAggregate) result() (driver.Value, error) {
	if a.err != nil {
		return nil, a.err
	}
	if !a.seen {
		return nil, nil
	}
	value := new(big.Rat).Set(&a.sum)
	if a.average {
		value.Quo(value, new(big.Rat).SetInt64(a.count))
		result, err := rounded(value, a.scale)
		if err != nil {
			return nil, err
		}
		return result.text(), nil
	}
	result, err := decimalFromRat(value)
	if err != nil {
		return nil, err
	}
	return result.text(), nil
}
