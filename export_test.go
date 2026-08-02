package tacklr

import (
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
)

// keep

// customInterrupt is only for RegisterInterrupt coverage.
type customInterrupt struct{}

func (c *customInterrupt) TypeName() string           { return "test_custom_interrupt_cov" }
func (c *customInterrupt) Serialize() ([]byte, error) { return []byte(`{}`), nil }
func (c *customInterrupt) Return([]byte) error        { return nil }
func (c *customInterrupt) Error() string              { return "custom" }

// TestRegisterInterrupt_customType is the public re-export outcome: hosts can
// register interrupt factories for checkpoint rehydrate.
func TestRegisterInterrupt_customType(t *testing.T) {
	RegisterInterrupt(func() Interrupt { return &customInterrupt{} })
	intr, ok := interrupt.New("test_custom_interrupt_cov")
	if !ok || intr == nil {
		t.Fatal("registered custom interrupt not found")
	}
	if intr.TypeName() != "test_custom_interrupt_cov" {
		t.Fatal(intr.TypeName())
	}
}
