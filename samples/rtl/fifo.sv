module fifo #(parameter WIDTH = 8, DEPTH = 4)(
  input logic clk, reset, push, pop,
  input logic [WIDTH-1:0] in_data,
  output logic [WIDTH-1:0] out_data,
  output logic full, empty
);
  localparam PTR_W = $clog2(DEPTH);
  logic [WIDTH-1:0] mem[DEPTH];
  logic [PTR_W-1:0] rd, wr;
  logic [PTR_W:0] count;
  assign full = count == DEPTH;
  assign empty = count == 0;
  assign out_data = mem[rd];
  wire take_push = push && !full;
  wire take_pop = pop && !empty;
  always_ff @(posedge clk) begin
    if (reset) begin rd <= 0; wr <= 0; count <= 0; end
    else begin
      if (take_push) begin mem[wr] <= in_data; wr <= wr + 1'b1; end
      if (take_pop) rd <= rd + 1'b1;
      case ({take_push, take_pop})
        2'b10: count <= count + 1'b1;
        2'b01: count <= count - 1'b1;
        default: ;
      endcase
    end
  end
endmodule
